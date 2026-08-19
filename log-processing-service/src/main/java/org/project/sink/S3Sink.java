package org.project.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.project.model.LogEvent;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

public class S3Sink extends RichSinkFunction<LogEvent> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);

    private static final int MAX_BATCH_LOGS = 7_000;
    private static final long MAX_BATCH_SIZE_BYTES = 64L * 1024 * 1024; // 64 MB
    private static final long FLUSH_INTERVAL_MS = 60_000; // 60 seconds

    private static final long TIME_CHECK_INTERVAL_MS = 10_000;

    private static final int MAX_PUT_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 200;
    private static final long MAX_BACKOFF_MS = 5_000;
    private static final long SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS = 180;

    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);

    private final String bucket;
    private final String endpointOverride;
    private final String accessKey;
    private final String secretKey;

    private transient S3Client s3Client;
    private transient ObjectMapper objectMapper;
    private transient int subtaskIndex;

    private transient Object lock;
    private transient Map<BatchKey, Batch> batches;

    private transient ScheduledExecutorService flushScheduler;

    private transient AtomicReference<Throwable> asyncFlushError;

    private transient Counter recordsWrittenCounter;
    private transient Counter bytesWrittenCounter;
    private transient Counter batchesFlushedCounter;
    private transient Counter flushFailuresCounter;
    private transient Counter malformedRecordsCounter;
    private transient Counter droppedRecordsCounter;

    public S3Sink(String bucket, String endpointOverride, String accessKey, String secretKey) {
        this.bucket = bucket;
        this.endpointOverride = endpointOverride;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        s3Client = buildS3Client();

        subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        lock = new Object();
        batches = new HashMap<>();
        asyncFlushError = new AtomicReference<>();

        MetricGroup metrics = getRuntimeContext().getMetricGroup();
        recordsWrittenCounter = metrics.counter("recordsWritten");
        bytesWrittenCounter = metrics.counter("bytesWritten");
        batchesFlushedCounter = metrics.counter("batchesFlushed");
        flushFailuresCounter = metrics.counter("flushFailures");
        malformedRecordsCounter = metrics.counter("malformedRecords");
        droppedRecordsCounter = metrics.counter("droppedRecords");

        // Implemented using Scheduled Thread Pool / Scheduled Executor Service (simply, java Scheduler)
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "s3-sink-flush-timer-" + subtaskIndex);
            t.setDaemon(true);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(
                this::scheduledFlushTick, TIME_CHECK_INTERVAL_MS, TIME_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(LogEvent log, Context context) throws Exception {
        checkAsyncError();

        byte[] json;
        try {
            json = objectMapper.writeValueAsBytes(log);
        } catch (Exception e) {
            LOG.error("Dropping unserializable LogEvent logId={}: {}", log.getLogId(), String.valueOf(e), e);
            malformedRecordsCounter.inc();
            droppedRecordsCounter.inc();
            // It is highly unlikely to a record to come here.
            // If it comes till here, dropping it and printing it in logs is the right way to do it.
            return;
        }

        BatchKey key = deriveBatchKey(log);
        Batch overflowed = null;

        synchronized (lock) {
            Batch batch = batches.computeIfAbsent(key, k -> new Batch());
            if (batch.wouldOverflow(json.length)) {
                overflowed = batches.remove(key);
                batch = batches.computeIfAbsent(key, k -> new Batch());
            }
            batch.add(json);
        }

        if (overflowed != null) {
            flushBatch(key, overflowed);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to restore: batches are always fully flushed to S3 before a
        // checkpoint completes (see snapshotState), so there is never anything
        // pending in Flink-managed state to recover after a restart.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkAsyncError();
        flushAll();
    }

    @Override
    public void close() throws Exception {
        Exception firstError = null;
        try {
            if (flushScheduler != null) {
                flushScheduler.shutdown();
                try {
                    if (!flushScheduler.awaitTermination(SCHEDULER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        flushScheduler.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    flushScheduler.shutdownNow();
                }
            }
            checkAsyncError();
            flushAll();
        } catch (Exception e) {
            firstError = e;
        } finally {
            if (s3Client != null) {
                try {
                    s3Client.close();
                } catch (Exception e) {
                    if (firstError == null) {
                        firstError = e;
                    } else {
                        firstError.addSuppressed(e);
                    }
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    private S3Client buildS3Client() {
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(30))
                .apiCallAttemptTimeout(Duration.ofSeconds(10))
                .build();

        S3ClientBuilder builder = S3Client.builder()
                .endpointOverride(URI.create(endpointOverride))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .overrideConfiguration(overrideConfig);

        if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        return builder.build();
    }

    private BatchKey deriveBatchKey(LogEvent log) {
        boolean malformed = false;

        String host = log.getHost();
        if (host == null || host.isBlank()) {
            host = "unknown-host";
            malformed = true;
        } else {
            host = sanitizeForKey(host);
        }

        String service = log.getService();
        if (service == null || service.isBlank()) {
            service = "unknown-service";
            malformed = true;
        } else {
            service = sanitizeForKey(service);
        }

        String datePath;
        Instant timestamp = log.getTimestamp();
        if (timestamp == null) {
            datePath = DATE_FORMATTER.format(Instant.now());
            malformed = true;
        } else {
            datePath = DATE_FORMATTER.format(timestamp);
        }

        if (malformed) {
            malformedRecordsCounter.inc();
            LOG.warn("LogEvent logId={} missing host/service/timestamp; routed as host={} service={} date={}",
                    log.getLogId(), host, service, datePath);
        }

        return new BatchKey(host, service, datePath);
    }

    private static String sanitizeForKey(String value) {
        String cleaned = value.replaceAll("[^a-zA-Z0-9_.-]", "_");
        //unknown value is not possible but for better safety I added it.
        return cleaned.isEmpty() ? "unknown" : cleaned;
    }

    private String buildObjectKey(BatchKey key) {
        String uniqueSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        return String.format(
                "logs/%s/%s/%s/%d-%d-%s.json.gz",
                key.host, key.service, key.datePath, subtaskIndex, System.currentTimeMillis(), uniqueSuffix);
    }

    private void flushBatch(BatchKey key, Batch batch) throws IOException {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        byte[] compressed = compress(batch.records);
        String objectKey = buildObjectKey(key);

        putWithRetry(objectKey, compressed);

        recordsWrittenCounter.inc(batch.records.size());
        bytesWrittenCounter.inc(compressed.length);
        batchesFlushedCounter.inc();
    }

    private void flushAll() throws IOException {
        List<Map.Entry<BatchKey, Batch>> snapshot;
        synchronized (lock) {
            snapshot = new ArrayList<>(batches.entrySet());
            batches.clear();
        }

        IOException firstError = null;
        for (Map.Entry<BatchKey, Batch> entry : snapshot) {
            try {
                flushBatch(entry.getKey(), entry.getValue());
            } catch (IOException e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }

    //actual saving is happening here.
    private void putWithRetry(String objectKey, byte[] data) throws IOException {
        int attempt = 0;
        long backoffMs = INITIAL_BACKOFF_MS;

        while (true) {
            try {
                //request is just metadata only. RequestBody.fromBytes(data) is actual data.
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .contentType("application/x-ndjson")
                        .contentEncoding("gzip")
                        .build();
                s3Client.putObject(request, RequestBody.fromBytes(data));
                return;
            } catch (SdkException e) {
                attempt++;
                boolean willRetry = attempt <= MAX_PUT_RETRIES && isRetryable(e);
                if (!willRetry) {
                    flushFailuresCounter.inc();
                    throw new IOException("Failed to upload batch to S3 after " + attempt + " attempt(s), key=" + objectKey, e);
                }
                LOG.warn("Transient error uploading {} to S3 (attempt {}/{}); retrying in {} ms: {}",
                        objectKey, attempt, MAX_PUT_RETRIES, backoffMs, String.valueOf(e));
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    private static boolean isRetryable(SdkException e) {
        if (e instanceof AwsServiceException) {
            //Reached AWS but got rejected.
            int statusCode = ((AwsServiceException) e).statusCode();
            return statusCode == 429 || statusCode >= 500;
        }
        //Maybe it is a network issue.
        return e instanceof SdkClientException;
    }

    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while backing off before S3 retry", ie);
        }
    }

    private static byte[] compress(List<byte[]> jsonRecords) throws IOException {
        //adding the data to the outputstream with GZIP compressor and adding new line after each
        // log
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(outputStream)) {
            for (byte[] json : jsonRecords) {
                gzip.write(json);
                gzip.write('\n');
            }
        }
        return outputStream.toByteArray();
    }

    private void scheduledFlushTick() {
        try {
            long now = System.currentTimeMillis();
            List<Map.Entry<BatchKey, Batch>> stale = new ArrayList<>();

            synchronized (lock) {
                Iterator<Map.Entry<BatchKey, Batch>> it = batches.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<BatchKey, Batch> entry = it.next();
                    if (entry.getValue().isStale(now)) {
                        stale.add(entry);
                        it.remove();
                    }
                }
            }

            Throwable firstError = null;

            for (Map.Entry<BatchKey, Batch> entry : stale) {
                try {
                    flushBatch(entry.getKey(), entry.getValue());
                } catch (Throwable t) {
                    if (firstError == null) {
                        firstError = t;
                    } else {
                        firstError.addSuppressed(t);
                    }
                }
            }

            if (firstError != null) {
                throw firstError;
            }
        } catch (Throwable t) {
            LOG.error("Fatal error during background time-based flush; sink will fail on the next "
                    + "record or checkpoint", t);
            asyncFlushError.compareAndSet(null, t);
        }
    }

    private void checkAsyncError() throws IOException {
        Throwable t = asyncFlushError.get();
        if (t != null) {
            throw new IOException("Failing task due to an earlier asynchronous flush error", t);
        }
    }

    //Each BatchKey is a filename and Each Batch is like a container with logs.
    //HashMap relationship <key, value> = <BatchKey, Batch>
    private static final class BatchKey {
        private final String host;
        private final String service;
        private final String datePath;

        private BatchKey(String host, String service, String datePath) {
            this.host = host;
            this.service = service;
            this.datePath = datePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BatchKey)) {
                return false;
            }
            BatchKey that = (BatchKey) o;
            return host.equals(that.host) && service.equals(that.service) && datePath.equals(that.datePath);
        }

        //Since HashMap is big, it is better to add hashCode for faster HashMap changes.
        @Override
        public int hashCode() {
            return Objects.hash(host, service, datePath);
        }
    }

    private static final class Batch {
        private final List<byte[]> records = new ArrayList<>();
        private final long startTimeMillis = System.currentTimeMillis(); //From 1 Jan 1970 UTC
        private long sizeBytes = 0L;

        boolean isEmpty() {
            return records.isEmpty();
        }

        boolean wouldOverflow(int nextRecordBytes) {
            if (records.isEmpty()) {
                return false;
            }
            return records.size() >= MAX_BATCH_LOGS || (sizeBytes + nextRecordBytes) >= MAX_BATCH_SIZE_BYTES;
        }

        boolean isStale(long nowMillis) {
            return !records.isEmpty() && (nowMillis - startTimeMillis) >= FLUSH_INTERVAL_MS;
        }

        void add(byte[] json) {
            records.add(json);
            sizeBytes += json.length;
        }
    }
}