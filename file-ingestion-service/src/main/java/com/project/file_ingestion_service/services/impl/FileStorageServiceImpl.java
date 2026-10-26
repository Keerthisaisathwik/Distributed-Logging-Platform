package com.project.file_ingestion_service.services.impl;

import com.project.file_ingestion_service.services.FileCompletionTracker;
import com.project.file_ingestion_service.services.FileStorageService;
import com.project.file_ingestion_service.services.StorageQuotaService;
import com.project.file_ingestion_service.services.UploadRequestTooLargeException;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.Comparator;
import java.util.stream.Stream;

@Service
public class FileStorageServiceImpl implements FileStorageService {

    //Need to add these two variables to the application.properties
    private static final int MAX_FILES = 10;
    private static final long MAX_REQUEST_BYTES = 5L * 1024 * 1024;

    private static final Set<String> SUPPORTED_EXTENSIONS = Set.of(".log", ".json", ".csv", ".ndjson", ".txt", ".xml", ".yaml", ".yml", ".properties", ".conf", ".ini", ".tsv", ".sql");

    private final Path uploadRoot;
    private final Path stagingRoot;

    private final StorageQuotaService quota;
    private final FileCompletionTracker completionTracker;

    public FileStorageServiceImpl(
            @Value("${file.storage.root:/app/uploads/logs}")
            String uploadRoot,
            StorageQuotaService quota,
            FileCompletionTracker completionTracker) {

        this.uploadRoot = Path.of(uploadRoot).toAbsolutePath().normalize();

        this.stagingRoot = this.uploadRoot.getParent().resolve("." + this.uploadRoot.getFileName() + "-staging");

        this.quota = quota;
        this.completionTracker = completionTracker;
    }

    @PostConstruct
    public void initializeStaging() {
        try {
            // A previous process may have crashed during an upload.
            // The staging directory belongs to this application instance.
            deleteRecursively(stagingRoot);
            Files.createDirectories(stagingRoot);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to initialize upload staging", e);
        }
    }

    @Override
    public Mono<Void> save(
            FilePart filePart,
            String clientId,
            String tokenId) {

        return saveBatch(List.of(filePart), clientId, tokenId);
    }

    @Override
    public Mono<Void> saveBatch(
            List<FilePart> fileParts,
            String clientId,
            String tokenId) {

        if (fileParts == null || fileParts.isEmpty()) {
            return Mono.error(
                    new IllegalArgumentException(
                            "At least one file is required"));
        }

        if (fileParts.size() > MAX_FILES) {
            return Mono.error(
                    new IllegalArgumentException(
                            "Maximum 10 files are allowed"));
        }

        final String safeClientId;

        try {
            safeClientId = validateClientId(clientId);
        } catch (RuntimeException e) {
            return Mono.error(e);
        }

        String batchId = UUID.randomUUID().toString();

        Path clientDirectory = uploadRoot.resolve(safeClientId)
                .normalize();

        if (!clientDirectory.startsWith(uploadRoot)) {
            return Mono.error(
                    new IllegalArgumentException("Invalid client ID"));
        }

        UploadContext context = new UploadContext(
                clientDirectory,
                stagingRoot.resolve(batchId),
                clientDirectory.resolve(batchId)
        );

        // Stage 1: Prepare the directories.
        Mono<Void> prepare = Mono.fromCallable(() -> {
                    Files.createDirectories(uploadRoot);
                    Files.createDirectories(clientDirectory);
                    Files.createDirectories(stagingRoot);
                    Files.createDirectory(context.stagingDirectory);
                    return Boolean.TRUE;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();

        // Stage 2: Save the files sequentially.
        Mono<Void> stage = Flux.fromIterable(fileParts)
                .concatMap((FilePart file) ->
                        stageFile(file, context))
                .then();

        // Stage 3: Publish the completed batch.
        Mono<Void> publish = Mono.<Void>fromRunnable(
                        () -> publishBatch(context))
                .subscribeOn(Schedulers.boundedElastic());

        // Execute each stage in order.
        Mono<Void> work = prepare
                .then(stage)
                .then(publish);

        return work.onErrorResume(
                error -> cleanupAfterFailure(context, error));
    }

    private Mono<Void> stageFile(
            FilePart filePart,
            UploadContext context) {

        return Mono.fromCallable(() -> {
                    String filename = createSafeFilename(
                            filePart.filename());

                    Path staged = context.stagingDirectory
                            .resolve(filename);

                    Path destination = context.finalBatchDirectory
                            .resolve(filename);

                    return new FileEntry(staged, destination, 0);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(entry ->
                        writeFilePart(filePart, entry.stagedPath, context)
                                .then(
                                        Mono.fromCallable(() ->
                                                        countRecords(
                                                                entry.stagedPath))
                                                .subscribeOn(
                                                        Schedulers.boundedElastic())
                                )
                                .flatMap(recordCount -> {
                                    if (recordCount == 0) {
                                        return Mono.fromCallable(() -> {
                                                    Files.deleteIfExists(entry.stagedPath);
                                                    return true;
                                                })
                                                .subscribeOn(Schedulers.boundedElastic())
                                                .then(Mono.<Void>empty());
                                    }

                                    context.files.add(
                                            new FileEntry(
                                                    entry.stagedPath,
                                                    entry.finalPath,
                                                    recordCount));

                                    return Mono.<Void>empty();
                                })
                );
    }

    private Mono<Void> writeFilePart(
            FilePart filePart,
            Path destination,
            UploadContext context) {

        return Mono.<Void, OutputStream>using(
                        () -> openOutputStream(destination),

                        output -> filePart.content()
                                .concatMap(buffer ->
                                                Mono.fromRunnable(() -> {
                                                            try {
                                                                int length = buffer.readableByteCount();

                                                                reserveChunk(context, length);

                                                                byte[] bytes = new byte[length];
                                                                buffer.read(bytes);
                                                                output.write(bytes);

                                                            } catch (IOException e) {
                                                                throw new UncheckedIOException(e);
                                                            } finally {
                                                                DataBufferUtils.release(buffer);
                                                            }
                                                        })
                                                        .subscribeOn(Schedulers.boundedElastic())
                                                        .then(),
                                        1
                                )
                                .then(),

                        FileStorageServiceImpl::closeOutputStream
                )
                .subscribeOn(Schedulers.boundedElastic())
                .doOnDiscard(
                        DataBuffer.class,
                        DataBufferUtils::release
                );
    }

    private void reserveChunk(
            UploadContext context,
            int bytes) {

        synchronized (context) {
            if (bytes > MAX_REQUEST_BYTES - context.totalBytes) {
                throw new UploadRequestTooLargeException();
            }

            // Reserve before writing this chunk to disk.
            quota.reserveBytes(bytes);

            context.totalBytes += bytes;
            context.reservedBytes += bytes;
        }
    }

    private void publishBatch(UploadContext context) {
        try {
            if (context.files.isEmpty()) {
                // All supplied files were empty. Nothing needs publishing.
                quota.commitReservedBytes(context.reservedBytes);
                context.quotaCommitted = true;
                deleteRecursively(context.stagingDirectory);
                return;
            }

            // Register completion state before making files visible to Tail.
            for (FileEntry file : context.files) {
                completionTracker.registerFile(
                        file.finalPath,
                        file.expectedRecords);
            }

            // Count the bytes as stored before Fluent Bit can read them.
            quota.commitReservedBytes(context.reservedBytes);
            context.quotaCommitted = true;

            // The staging and upload directories must be on the same
            // filesystem. Do not publish individual files one at a time.
            Files.move(
                    context.stagingDirectory,
                    context.finalBatchDirectory,
                    StandardCopyOption.ATOMIC_MOVE
            );

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Mono<Void> cleanupAfterFailure(
            UploadContext context,
            Throwable originalError) {

        return Mono.fromRunnable(() -> {
                    try {
                        long remainingBytes =
                                directorySize(context.stagingDirectory)
                                        + directorySize(context.finalBatchDirectory);

                        deleteRecursively(context.stagingDirectory);
                        deleteRecursively(context.finalBatchDirectory);

                        for (FileEntry file : context.files) {
                            completionTracker.unregisterFile(file.finalPath);
                        }

                        if (context.quotaCommitted) {
                            if (remainingBytes > 0) {
                                quota.fileDeleted(remainingBytes);
                            }
                        } else if (context.reservedBytes > 0) {
                            quota.releaseReservedBytes(context.reservedBytes);
                        }

                    } catch (Exception cleanupError) {
                        originalError.addSuppressed(cleanupError);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then(Mono.<Void>error(originalError));
    }

    private long countRecords(Path file) throws IOException {
        // Fluent Bit Tail emits newline-delimited records.
        try (Stream<String> lines =
                     Files.lines(file, StandardCharsets.UTF_8)) {
            return lines.count();
        }
    }

    private String validateClientId(String clientId) {
        if (clientId == null
                || !clientId.matches("[A-Za-z0-9._-]{1,128}")
                || clientId.equals(".")
                || clientId.equals("..")) {

            throw new IllegalArgumentException("Invalid client ID");
        }

        return clientId;
    }

    private String createSafeFilename(String originalFilename) {
        if (originalFilename == null || originalFilename.isBlank()) {
            throw new IllegalArgumentException("Filename is required");
        }

        String normalized = originalFilename.replace('\\', '/');
        String base = normalized.substring(
                normalized.lastIndexOf('/') + 1);

        base = base.replaceAll("[^A-Za-z0-9._-]", "_");

        if (base.isBlank() || base.equals(".") || base.equals("..")) {
            throw new IllegalArgumentException("Invalid filename");
        }

        int dot = base.lastIndexOf('.');

        if (dot <= 0 || dot == base.length() - 1) {
            throw new IllegalArgumentException(
                    "File extension is required");
        }

        String extension = base.substring(dot).toLowerCase(Locale.ROOT);

        if (!SUPPORTED_EXTENSIONS.contains(extension)) {
            throw new IllegalArgumentException(
                    "Unsupported file extension: " + extension);
        }

        return UUID.randomUUID() + "_" + base;
    }

    private static OutputStream openOutputStream(Path path) {
        try {
            return Files.newOutputStream(
                    path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void closeOutputStream(OutputStream output) {
        try {
            output.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long directorySize(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return 0;
        }

        try (Stream<Path> paths = Files.walk(directory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .sum();
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        try (Stream<Path> paths = Files.walk(path)) {
            for (Path item : paths
                    .sorted(Comparator.reverseOrder())
                    .toList()) {
                Files.deleteIfExists(item);
            }
        }
    }

    private static final class UploadContext {
        private final Path clientDirectory;
        private final Path stagingDirectory;
        private final Path finalBatchDirectory;

        private final List<FileEntry> files = new ArrayList<>();

        private long totalBytes;
        private long reservedBytes;
        private boolean quotaCommitted;

        private UploadContext(Path clientDirectory, Path stagingDirectory, Path finalBatchDirectory) {
            this.clientDirectory = clientDirectory;
            this.stagingDirectory = stagingDirectory;
            this.finalBatchDirectory = finalBatchDirectory;
        }
    }

    private static final class FileEntry {
        private final Path stagedPath;
        private final Path finalPath;
        private final long expectedRecords;

        private FileEntry(Path stagedPath, Path finalPath, long expectedRecords) {
            this.stagedPath = stagedPath;
            this.finalPath = finalPath;
            this.expectedRecords = expectedRecords;
        }
    }
}