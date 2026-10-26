package com.project.file_ingestion_service.services;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

@Slf4j
@Component
public class FileCompletionTracker {

    public static final String SOURCE_FILE = "_source_file";
    public static final String SOURCE_OFFSET = "_source_offset";

    private final Path uploadRoot;
    private final Path metadataRoot;
    private final StorageQuotaService quota;

    private final Base64.Encoder encoder =
            Base64.getUrlEncoder().withoutPadding();
    private final Base64.Decoder decoder =
            Base64.getUrlDecoder();

    public FileCompletionTracker(
            @Value("${file.storage.root:/app/uploads/logs}")
            String uploadRoot,
            StorageQuotaService quota) {

        this.uploadRoot = Path.of(uploadRoot)
                .toAbsolutePath()
                .normalize();

        this.metadataRoot = this.uploadRoot.resolveSibling(
                this.uploadRoot.getFileName() + ".metadata");

        this.quota = quota;
    }

    @PostConstruct
    public synchronized void initialize() {
        try {
            Files.createDirectories(metadataRoot);

            List<Path> manifests;

            try (Stream<Path> paths = Files.list(metadataRoot)) {
                manifests = paths
                        .filter(p -> p.getFileName().toString()
                                .endsWith(".state"))
                        .toList();
            }

            for (Path manifest : manifests) {
                State state = readState(manifest);

                if (state.processedOffsets.size()
                        >= state.expectedRecords) {
                    completeFile(manifest, state);
                } else if (!Files.exists(state.sourceFile)) {
                    // A crash may have occurred before publication.
                    Files.deleteIfExists(manifest);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to recover file-processing state", e);
        }
    }

    /**
     * Called before a completed file is published to Fluent Bit.
     */
    public synchronized void registerFile(
            Path sourceFile,
            long expectedRecords) throws IOException {

        Path normalized = validateSourcePath(sourceFile);

        if (expectedRecords <= 0) {
            return;
        }

        State state = new State(normalized, expectedRecords);
        writeState(stateFileFor(normalized), state);
    }

    /**
     * Called only after every accepted record in the HTTP batch
     * has been acknowledged by Kafka.
     *
     * Filtered records should be included too, because they were
     * intentionally discarded by the application.
     */
    public synchronized void markProcessed(
            List<Map<String, Object>> records) throws IOException {

        Map<Path, Set<String>> offsetsByFile = new LinkedHashMap<>();

        for (Map<String, Object> record : records) {
            Object sourceValue = record.get(SOURCE_FILE);
            Object offsetValue = record.get(SOURCE_OFFSET);

            if (sourceValue == null || offsetValue == null) {
                // Missing metadata must never trigger file deletion.
                continue;
            }

            Path source;

            try {
                source = validateSourcePath(
                        Path.of(sourceValue.toString()));
            } catch (IllegalArgumentException e) {
                log.warn("Ignoring invalid source-file metadata");
                continue;
            }

            offsetsByFile
                    .computeIfAbsent(source, ignored -> new HashSet<>())
                    .add(offsetValue.toString());
        }

        for (Map.Entry<Path, Set<String>> entry
                : offsetsByFile.entrySet()) {

            Path source = entry.getKey();
            Path manifest = stateFileFor(source);

            if (!Files.exists(manifest)) {
                // No trustworthy completion manifest: retain the file.
                continue;
            }

            State state = readState(manifest);

            if (!state.sourceFile.equals(source)) {
                continue;
            }

            state.processedOffsets.addAll(entry.getValue());

            // Persist the acknowledged offsets before considering deletion.
            writeState(manifest, state);

            if (state.processedOffsets.size()
                    >= state.expectedRecords) {
                completeFile(manifest, state);
            }
        }
    }

    public synchronized void unregisterFile(
            Path sourceFile) throws IOException {
        Path normalized = validateSourcePath(sourceFile);
        Files.deleteIfExists(stateFileFor(normalized));
    }

    private void completeFile(Path manifest, State state)
            throws IOException {

        Path source = validateSourcePath(state.sourceFile);

        if (Files.exists(source)) {
            long size = Files.size(source);

            if (Files.deleteIfExists(source)) {
                // Free quota only when the file was actually deleted.
                quota.fileDeleted(size);
                log.info("Deleted fully processed file: {}", source);
            }
        }

        Files.deleteIfExists(manifest);

        // Remove the now-empty batch directory if possible.
        Path batchDirectory = source.getParent();

        if (batchDirectory != null
                && !batchDirectory.equals(uploadRoot)) {
            try {
                Files.deleteIfExists(batchDirectory);
            } catch (DirectoryNotEmptyException ignored) {
                // Other files in this batch are still being processed.
            }
        }
    }

    private Path validateSourcePath(Path path) {
        Path normalized = path.toAbsolutePath().normalize();

        if (!normalized.startsWith(uploadRoot)) {
            throw new IllegalArgumentException(
                    "Source path is outside upload storage");
        }

        return normalized;
    }

    private Path stateFileFor(Path sourceFile) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(sourceFile.toString()
                            .getBytes(StandardCharsets.UTF_8));

            StringBuilder hex = new StringBuilder();

            for (byte value : hash) {
                hex.append(String.format("%02x", value & 0xff));
            }

            return metadataRoot.resolve(hex + ".state");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void writeState(Path manifest, State state)
            throws IOException {

        List<String> lines = new ArrayList<>();

        lines.add("path=" + encode(state.sourceFile.toString()));
        lines.add("expected=" + state.expectedRecords);

        for (String offset : new TreeSet<>(state.processedOffsets)) {
            lines.add("offset=" + encode(offset));
        }

        Files.createDirectories(metadataRoot);

        Path temporary = Files.createTempFile(
                metadataRoot, "state-", ".tmp");

        try {
            Files.write(temporary, lines, StandardCharsets.UTF_8);

            try {
                Files.move(
                        temporary,
                        manifest,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING
                );
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(
                        temporary,
                        manifest,
                        StandardCopyOption.REPLACE_EXISTING
                );
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private State readState(Path manifest) throws IOException {
        Path source = null;
        long expected = -1;
        Set<String> offsets = new HashSet<>();

        for (String line : Files.readAllLines(
                manifest, StandardCharsets.UTF_8)) {

            if (line.startsWith("path=")) {
                source = Path.of(decode(line.substring(5)))
                        .toAbsolutePath()
                        .normalize();
            } else if (line.startsWith("expected=")) {
                expected = Long.parseLong(line.substring(9));
            } else if (line.startsWith("offset=")) {
                offsets.add(decode(line.substring(7)));
            }
        }

        if (source == null || expected < 0) {
            throw new IOException(
                    "Invalid completion manifest: " + manifest);
        }

        return new State(source, expected, offsets);
    }

    private String encode(String value) {
        return encoder.encodeToString(
                value.getBytes(StandardCharsets.UTF_8));
    }

    private String decode(String value) {
        return new String(
                decoder.decode(value), StandardCharsets.UTF_8);
    }

    private static final class State {
        private final Path sourceFile;
        private final long expectedRecords;
        private final Set<String> processedOffsets;

        private State(Path sourceFile, long expectedRecords) {
            this(sourceFile, expectedRecords, new HashSet<>());
        }

        private State(
                Path sourceFile,
                long expectedRecords,
                Set<String> processedOffsets) {
            this.sourceFile = sourceFile;
            this.expectedRecords = expectedRecords;
            this.processedOffsets = processedOffsets;
        }
    }
}