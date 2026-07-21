package com.project.file_ingestion_service.services;

import org.reactivestreams.Publisher;
import org.springframework.http.codec.multipart.FilePart;

public interface FileStorageService {
    Publisher<?> save(FilePart file, String clientId, String tokenId);
}
