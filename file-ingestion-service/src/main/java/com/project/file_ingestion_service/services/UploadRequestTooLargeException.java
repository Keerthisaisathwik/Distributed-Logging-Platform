package com.project.file_ingestion_service.services;

public class UploadRequestTooLargeException extends RuntimeException {

    public UploadRequestTooLargeException() {
        super("Combined upload size must not exceed 5 MiB");
    }
}