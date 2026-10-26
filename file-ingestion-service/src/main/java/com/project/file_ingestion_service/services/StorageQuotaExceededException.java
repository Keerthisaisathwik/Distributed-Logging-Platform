package com.project.file_ingestion_service.services;

public class StorageQuotaExceededException extends RuntimeException {

    public StorageQuotaExceededException() {
        super("Instance storage limit of 200 MiB exceeded");
    }
}