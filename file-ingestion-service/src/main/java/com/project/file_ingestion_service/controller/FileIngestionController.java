package com.project.file_ingestion_service.controller;

import com.project.file_ingestion_service.services.FileStorageService;
import com.project.file_ingestion_service.services.StorageQuotaExceededException;
import com.project.file_ingestion_service.services.UploadRequestTooLargeException;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/files")
@RequiredArgsConstructor
public class FileIngestionController {

    private final FileStorageService fileStorageService;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<ResponseEntity<String>> uploadFiles(
            @RequestPart String clientId,
            @RequestPart String tokenId,
            @RequestPart("files") Flux<FilePart> files) {

        return files.collectList()
                .flatMap(fileList -> {
                    if (fileList.isEmpty()) {
                        return Mono.just(ResponseEntity.<String>badRequest().body("At least one file is required"));
                    }

                    if (fileList.size() > 10) {
                        return Mono.just(ResponseEntity.<String>badRequest().body("Maximum 10 files are allowed"));
                    }

                    return fileStorageService.saveBatch(fileList, clientId, tokenId)
                            .thenReturn(ResponseEntity.<String>ok().body("Files uploaded successfully"))
                            .onErrorResume(UploadRequestTooLargeException.class, error -> Mono.just(ResponseEntity.<String>status(HttpStatus.PAYLOAD_TOO_LARGE).body("Combined file size must not exceed 5 MiB")))
                            .onErrorResume(StorageQuotaExceededException.class, error -> Mono.just(ResponseEntity.<String>status(HttpStatus.INSUFFICIENT_STORAGE).body("Upload rejected: instance storage limit is 200 MiB")))
                            .onErrorResume(IllegalArgumentException.class, error -> Mono.just(ResponseEntity.<String>badRequest().body(error.getMessage())));
                });
    }
}