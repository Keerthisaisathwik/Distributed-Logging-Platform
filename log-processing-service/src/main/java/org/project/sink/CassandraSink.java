package org.project.sink;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.project.model.Environment;
import org.project.model.LogAttribute;
import org.project.model.LogEvent;
import org.project.model.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CassandraSink extends RichSinkFunction<LogEvent> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);

    // Heuristics used only by estimateBufferFootprintBytes() below -- see its javadoc.
    private static final int BYTES_PER_CHAR_ESTIMATE = 2;
    private static final int FIXED_OVERHEAD_BYTES_PER_EVENT = 256;

    // Configuration
    private final List<String> contactPoints;
    private final int port;
    private final String localDataCenter;
    private final String keyspace;
    private final int maxBatchLogEvents;        // Event-level batch count
    private final long maxBatchSizeBytes;        // Rough memory guard
    private final long flushIntervalMs;
    private final int maxConcurrentRequests;
    private final int maxRetries;

    // Driver & Statements
    private transient CqlSession session;
    private transient PreparedStatement serviceStatement;
    private transient PreparedStatement traceStatement;
    private transient PreparedStatement levelStatement;
    private transient PreparedStatement idStatement;

    // Buffers & Async State
    private transient List<LogEvent> eventBuffer;
    private transient long currentBufferBytes;
    private transient List<CompletableFuture<AsyncResultSet>> pendingFutures;
    private transient Semaphore concurrencySemaphore;
    private transient ScheduledExecutorService retryExecutor;

    // Error Propagation & Timers
    private transient AtomicReference<Throwable> asyncError;
    private transient AtomicInteger inFlightOperationsCount;
    private transient long lastFlushTime;

    // Metrics
    private transient Counter successCounter;
    private transient Counter failureCounter;
    private transient Counter retryCounter;
    private transient Counter malformedLogIdCounter;
    private transient Counter droppedAttributesCounter;

    public CassandraSink(
            String contactPointsStr,
            int port,
            String localDataCenter,
            String keyspace,
            int maxBatchLogEvents,
            long maxBatchSizeBytes,
            long flushIntervalMs,
            int maxConcurrentRequests,
            int maxRetries
    ) {
        this.contactPoints = parseContactPoints(contactPointsStr);
        this.port = port;
        this.localDataCenter = localDataCenter;
        this.keyspace = keyspace;
        this.maxBatchLogEvents = maxBatchLogEvents;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.flushIntervalMs = flushIntervalMs;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.maxRetries = maxRetries;
    }

    private static List<String> parseContactPoints(String contactPointsStr) {
        if (contactPointsStr == null || contactPointsStr.isBlank()) {
            throw new IllegalArgumentException("Contact points string cannot be null or empty");
        }
        return Arrays.stream(contactPointsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        List<InetSocketAddress> socketAddresses = contactPoints.stream()
                .map(host -> new InetSocketAddress(host.trim(), port))
                .collect(Collectors.toList());

        session = CqlSession.builder()
                .addContactPoints(socketAddresses)
                .withLocalDatacenter(localDataCenter)
                .withKeyspace(keyspace)
                .build();

        serviceStatement = session.prepare("""
            INSERT INTO logs_by_service
            (service_name, log_date, timestamp, log_id, log_level, host, env, message, namespace, pod_name, trace_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """);

        traceStatement = session.prepare("""
            INSERT INTO logs_by_traceid
            (trace_id, timestamp, log_id, host, service_name, log_level, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """);

        levelStatement = session.prepare("""
            INSERT INTO logs_by_level
            (log_level, log_date, timestamp, log_id, host, service_name, message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """);

        idStatement = session.prepare("""
            INSERT INTO logs_by_id
            (host, log_id, env, timestamp, service_name, instance_id, namespace, pod_name, trace_id, log_level, message, attributes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """);

        eventBuffer = new ArrayList<>(maxBatchLogEvents);
        currentBufferBytes = 0;

        pendingFutures = new ArrayList<>();
        concurrencySemaphore = new Semaphore(maxConcurrentRequests);
        retryExecutor = Executors.newScheduledThreadPool(Math.min(maxConcurrentRequests, 4));

        asyncError = new AtomicReference<>(null);
        inFlightOperationsCount = new AtomicInteger(0);
        lastFlushTime = System.currentTimeMillis();

        // Metrics registration
        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        successCounter = runtimeContext.getMetricGroup().counter("cassandra_writes_success");
        failureCounter = runtimeContext.getMetricGroup().counter("cassandra_writes_failed");
        retryCounter = runtimeContext.getMetricGroup().counter("cassandra_write_retries");
        malformedLogIdCounter = runtimeContext.getMetricGroup().counter("cassandra_malformed_log_id");
        droppedAttributesCounter = runtimeContext.getMetricGroup().counter("cassandra_dropped_attributes");
        runtimeContext.getMetricGroup().gauge("cassandra_pending_requests", (Gauge<Integer>) inFlightOperationsCount::get);

        // Here we are using Flink Processing-Time Callback instead of java scheduler.
        runtimeContext.getProcessingTimeService().registerTimer(
                runtimeContext.getProcessingTimeService().getCurrentProcessingTime() + flushIntervalMs,
                this::onProcessingTimer
        );
    }

    @Override
    public void invoke(LogEvent log, Context context) throws Exception {
        checkAsyncError(); // Fail fast if a background write failed

        long estimatedSize = estimateBufferFootprintBytes(log);

        // Check if adding this log exceeds batch thresholds
        boolean countExceeded = eventBuffer.size() >= maxBatchLogEvents;
        boolean sizeExceeded = (currentBufferBytes + estimatedSize) >= maxBatchSizeBytes;

        if (countExceeded || sizeExceeded) {
            flush();
        }

        eventBuffer.add(log);
        currentBufferBytes += estimatedSize;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // Idempotent upserts do not require restored state
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("Flink Checkpoint {} starting: Flushing buffers and draining async Cassandra writes...", context.getCheckpointId());

        flush();
        drainAllFutures();
        checkAsyncError(); // Final check before acknowledging checkpoint success
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
            drainAllFutures();
            checkAsyncError(); // Surface a drain timeout/failure instead of swallowing it on shutdown
        } finally {
            if (retryExecutor != null) {
                retryExecutor.shutdown();
            }
            if (session != null) {
                session.close();
            }
        }
    }

    private synchronized void flush() throws Exception {
        checkAsyncError();

        if (eventBuffer.isEmpty()) {
            return;
        }

        List<BoundStatement> statementsToExecute = new ArrayList<>(eventBuffer.size() * 4);
        for (LogEvent log : eventBuffer) {
            //In the future, I'll need to change the logId with snowflake here I am making string to UUID conversion.
            UUID logId = UUID.fromString(log.getLogId());
            System.out.println("_________________________");
            System.out.println(log);
            System.out.println("_________________________");
            statementsToExecute.add(serviceStatement.bind(
                    log.getService(), log.getLogDate(), log.getTimestamp(), logId,
                    log.getSeverityLevel().toString(), log.getHost(), log.getEnv().toString(),
                    log.getMessage(), log.getNamespace(), log.getPodName(), log.getTraceId()
            ));

            if (log.getTraceId() != null) {
                statementsToExecute.add(traceStatement.bind(
                        log.getTraceId(), log.getTimestamp(), logId, log.getHost(),
                        log.getService(), log.getSeverityLevel().toString(), log.getMessage()
                ));
            }

            statementsToExecute.add(levelStatement.bind(
                    log.getSeverityLevel().toString(), log.getLogDate(), log.getTimestamp() ,logId,
                    log.getHost(), log.getService(), log.getMessage()
            ));

            BoundStatement idStmt = buildIdStatement(log);
            if (idStmt != null) {
                statementsToExecute.add(idStmt);
            }
        }

        for (BoundStatement stmt : statementsToExecute) {
            concurrencySemaphore.acquire(); // Intentionally block main thread for backpressure if database is slow
            inFlightOperationsCount.incrementAndGet();

            CompletableFuture<AsyncResultSet> future = executeWithRetry(stmt, 0);
            pendingFutures.add(future);
        }

        // Clear buffer state
        eventBuffer.clear();
        currentBufferBytes = 0;
        lastFlushTime = System.currentTimeMillis();

        // Safe cleanup: keep incomplete futures, verify no errors in completed ones
        cleanupCompletedFutures();
    }

    /**
     * Builds the bound statement for logs_by_id, or null if this event can't
     * be written to that table. logs_by_id.log_id is a uuid, but
     * LogEvent.getLogId() is a plain String -- if it doesn't parse as a UUID,
     * we drop just this table's row for the event (logging + counting it)
     * rather than throwing and failing the whole flush batch, since the
     * other three tables above have already been queued successfully.
     */
    private BoundStatement buildIdStatement(LogEvent log) {
        UUID logId;
        try {
            logId = UUID.fromString(log.getLogId());
        } catch (IllegalArgumentException | NullPointerException e) {
            LOG.error("Skipping logs_by_id write: logId '{}' is not a valid UUID (service={}, timestamp={})",
                    log.getLogId(), log.getService(), log.getTimestamp());
            malformedLogIdCounter.inc();
            return null;
        }

        BoundStatementBuilder builder = idStatement.boundStatementBuilder()
                .setString("host", log.getHost())
                .setUuid("log_id", logId)
                .setString("env", log.getEnv() != null ? log.getEnv().toString() : null)
                .setInstant("timestamp", log.getTimestamp())
                .setString("service_name", log.getService())
                .setString("instance_id", log.getInstanceId())
                .setString("namespace", log.getNamespace())
                .setString("pod_name", log.getPodName())
                .setString("trace_id", log.getTraceId())
                .setString("log_level", log.getSeverityLevel() != null ? log.getSeverityLevel().toString() : null)
                .setString("message", log.getMessage());

        Map<String, String> attributesMap = toAttributeMap(log.getAttributes());
        if (attributesMap != null && !attributesMap.isEmpty()) {
            builder = builder.setMap("attributes", attributesMap, String.class, String.class);
        }
        // else: leave the column unset rather than binding null/empty. CQL
        // treats an explicit null bind on INSERT as a real tombstone write,
        // and most log events won't carry attributes -- unset avoids
        // generating a tombstone for that cell on every such row.

        return builder.build();
    }

    /**
     * Converts the event's attribute list into the Map<String,String> the
     * attributes column expects. CQL maps reject null keys/values outright,
     * so entries missing either are dropped (and counted) rather than
     * failing the whole write. Duplicate keys keep the last occurrence,
     * matching normal Map.put semantics.
     */
    private Map<String, String> toAttributeMap(List<LogAttribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return null;
        }
        Map<String, String> map = new HashMap<>(attributes.size() * 2);
        for (LogAttribute attr : attributes) {
            if (attr == null || attr.getKey() == null || attr.getValue() == null) {
                droppedAttributesCounter.inc();
                LOG.debug("Dropping log attribute with null key/value: {}", attr);
                continue;
            }
            map.put(attr.getKey(), attr.getValue());
        }
        return map;
    }

    private CompletableFuture<AsyncResultSet> executeWithRetry(BoundStatement stmt, int attempt) {
        CompletionStage<AsyncResultSet> stage = session.executeAsync(stmt);
        CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();

        stage.whenComplete((result, ex) -> {
            if (ex == null) {
                successCounter.inc();
                inFlightOperationsCount.decrementAndGet();
                concurrencySemaphore.release();
                future.complete(result);
            } else {
                boolean retryable = isRetryable(ex);

                if (retryable && attempt < maxRetries) {
                    long backoffMs = Math.min(10_000L, (long) Math.pow(2, attempt) * 100L); // Exponential backoff
                    retryCounter.inc();
                    LOG.warn("Cassandra write failed (attempt {}/{}). Retrying in {} ms. Cause: {}",
                            attempt + 1, maxRetries, backoffMs, ex.toString());

                    retryExecutor.schedule(() -> {
                        executeWithRetry(stmt, attempt + 1)
                                .whenComplete((retryRes, retryEx) -> {
                                    if (retryEx != null) {
                                        future.completeExceptionally(retryEx);
                                    } else {
                                        future.complete(retryRes);
                                    }
                                });
                    }, backoffMs, TimeUnit.MILLISECONDS);
                } else {
                    // Either a permanent/non-retryable error, or retries exhausted.
                    failureCounter.inc();
                    inFlightOperationsCount.decrementAndGet();
                    concurrencySemaphore.release();
                    LOG.error("Fatal Cassandra write error after {} attempt(s) [{}]",
                            attempt + 1, retryable ? "retries exhausted" : "non-retryable error", ex);

                    // Set global error flag to fail Flink checkpoint/subtask
                    asyncError.compareAndSet(null, ex);
                    future.completeExceptionally(ex);
                }
            }
        });

        return future;
    }

    private boolean isRetryable(Throwable ex) {
        Throwable cause = (ex instanceof DriverExecutionException && ex.getCause() != null)
                ? ex.getCause()
                : ex;

        return !(cause instanceof QueryValidationException          // bad/invalid/unauthorized CQL: always fails the same way
                || cause instanceof AuthenticationException          // bad credentials
                || cause instanceof UnsupportedProtocolVersionException
                || cause instanceof InvalidKeyspaceException
                || cause instanceof CodecNotFoundException            // Java <-> CQL type mapping bug
                || cause instanceof ProtocolError                     // client-triggered protocol violation, a driver/client bug
                || cause instanceof FrameTooLongException              // payload too large; won't shrink on retry
                || cause instanceof IllegalArgumentException
                || cause instanceof IllegalStateException);
    }

    private void cleanupCompletedFutures() throws IOException {
        pendingFutures.removeIf(future -> {
            if (future.isDone()) {
                if (future.isCompletedExceptionally()) {
                    // Retain or trigger error handling
                    try {
                        future.getNow(null);
                    } catch (Exception e) {
                        asyncError.compareAndSet(null, e);
                    }
                }
                return true; // Remove safely only after inspecting result
            }
            return false;
        });
    }

    private void checkAsyncError() throws IOException {
        Throwable error = asyncError.get();
        if (error != null) {
            throw new IOException("Failing Flink task due to asynchronous Cassandra write failure", error);
        }
    }

    private void onProcessingTimer(long timestamp) throws Exception {
        synchronized (this) {
            if (System.currentTimeMillis() - lastFlushTime >= flushIntervalMs) {
                flush();
            }
        }
        // Re-register processing timer
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        context.getProcessingTimeService().registerTimer(
                timestamp + flushIntervalMs,
                this::onProcessingTimer
        );
    }

    private void drainAllFutures() throws Exception {
        if (pendingFutures.isEmpty()) {
            return;
        }

        // Snapshot what we're draining now so we can reliably reconcile
        // pendingFutures afterward no matter how the wait below ends
        // (success, timeout, or another exception).
        List<CompletableFuture<AsyncResultSet>> drainTarget = new ArrayList<>(pendingFutures);
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
                drainTarget.toArray(new CompletableFuture[0])
        );

        try {
            allOf.get(30, TimeUnit.SECONDS); // Timeout guard against hanging futures
        } catch (TimeoutException te) {
            long stillInFlight = drainTarget.stream().filter(f -> !f.isDone()).count();
            LOG.error("Timed out after 30s draining Cassandra writes: {} of {} operations still in " +
                            "flight with an unknown outcome. Failing this checkpoint/close so the pipeline " +
                            "replays the corresponding source records; the writes are idempotent upserts, " +
                            "so a replay converges to the correct state either way.",
                    stillInFlight, drainTarget.size());
            asyncError.compareAndSet(null, te);
        } catch (Exception e) {
            asyncError.compareAndSet(null, e);
        } finally {
            // Stop tracking everything we just attempted to drain. Completed
            // futures are done; any still-pending ones keep running against
            // the driver's own threads and will still correctly release the
            // semaphore, update counters, and set asyncError via the
            // callback chain in executeWithRetry -- we just stop waiting on
            // them here, so pendingFutures can't grow unbounded across
            // repeated timeouts.
            pendingFutures.removeAll(drainTarget);
        }
    }

    /**
     * A cheap, best-effort heuristic for how much heap a buffered LogEvent is
     * roughly costing us -- used ONLY to decide when eventBuffer has grown
     * large enough to trigger an early flush (maxBatchSizeBytes). This is
     * NOT a precise measurement of on-wire CQL size or true JVM heap usage:
     * it ignores object headers, the up-to-4 BoundStatements we later build
     * per event, and any LogEvent fields not listed below. (I don't have the
     * LogEvent source, so double-check this field list -- and that they're
     * really Strings -- against your actual class.) Treat maxBatchSizeBytes
     * as a guardrail against runaway buffering, not a byte-accurate budget.
     */
    private long estimateBufferFootprintBytes(LogEvent log) {
        if (log == null) {
            return 0;
        }
        long chars = 0;
        chars += strLen(log.getMessage());
        chars += strLen(log.getService());
        chars += strLen(log.getHost());
        chars += strLen(log.getEnv() != null ? log.getEnv().toString() : Environment.STAGING.toString());
        chars += strLen(log.getNamespace());
        chars += strLen(log.getPodName());
        chars += strLen(log.getTraceId());
        chars += strLen(log.getInstanceId());
        chars += strLen(log.getSeverityLevel() != null ? log.getSeverityLevel().toString() : LogLevel.INFO.toString());

        if (log.getAttributes() != null) {
            for (LogAttribute attr : log.getAttributes()) {
                if (attr != null) {
                    chars += strLen(attr.getKey());
                    chars += strLen(attr.getValue());
                }
            }
        }

        return FIXED_OVERHEAD_BYTES_PER_EVENT + chars * BYTES_PER_CHAR_ESTIMATE;
    }

    private static int strLen(String s) {
        return s != null ? s.length() : 0;
    }
}