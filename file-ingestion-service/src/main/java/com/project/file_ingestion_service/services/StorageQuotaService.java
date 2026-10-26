package com.project.file_ingestion_service.services;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@Component
public class StorageQuotaService {

    public static final long MAX_STORAGE_BYTES = 200L * 1024 * 1024;

    private final Path storageRoot;

    private long storedBytes;
    private long reservedBytes;

    public StorageQuotaService(
            @Value("${file.storage.root:/app/uploads/logs}")
            String storageRoot) {
        this.storageRoot = Path.of(storageRoot)
                .toAbsolutePath()
                .normalize();
    }

    @PostConstruct
    public synchronized void initialize() {
        try {
            Files.createDirectories(storageRoot);

            try (Stream<Path> paths = Files.walk(storageRoot)) {
                storedBytes = paths
                        .filter(Files::isRegularFile)
                        .mapToLong(this::fileSize)
                        .sum();
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Unable to initialize storage quota", e);
        }
    }

    public synchronized void reserveBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("Negative reservation");
        }

        if (bytes > MAX_STORAGE_BYTES - storedBytes - reservedBytes) {
            throw new StorageQuotaExceededException();
        }

        reservedBytes += bytes;
    }

    public synchronized void commitReservedBytes(long bytes) {
        if (bytes < 0 || bytes > reservedBytes) {
            throw new IllegalStateException("Invalid storage reservation");
        }

        reservedBytes -= bytes;
        storedBytes += bytes;
    }

    public synchronized void releaseReservedBytes(long bytes) {
        if (bytes < 0 || bytes > reservedBytes) {
            throw new IllegalStateException("Invalid reservation release");
        }

        reservedBytes -= bytes;
    }

    public synchronized void fileDeleted(long bytes) {
        if (bytes < 0 || bytes > storedBytes) {
            throw new IllegalStateException("Invalid storage decrement");
        }

        storedBytes -= bytes;
    }

    public synchronized long getStoredBytes() {
        return storedBytes;
    }

    public synchronized long getAvailableBytes() {
        return Math.max(
                0L, MAX_STORAGE_BYTES - storedBytes - reservedBytes);
    }

    private long fileSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}