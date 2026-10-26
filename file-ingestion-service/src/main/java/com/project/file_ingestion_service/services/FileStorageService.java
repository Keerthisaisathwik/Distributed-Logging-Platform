package com.project.file_ingestion_service.services;

import org.reactivestreams.Publisher;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

import java.util.List;

public interface FileStorageService {
    Publisher<?> save(FilePart file, String clientId, String tokenId);
    Mono<Void> saveBatch(List<FilePart> fileParts, String clientId, String tokenId);
}