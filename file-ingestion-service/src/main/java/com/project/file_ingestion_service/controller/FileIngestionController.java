package com.project.file_ingestion_service.controller;

import com.project.file_ingestion_service.services.FileStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/files")
@RequiredArgsConstructor
public class FileIngestionController {

    @Autowired
    private FileStorageService fileStorageService;

    @PostMapping(
            value = "/upload",
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE
    )
    public Mono<ResponseEntity<String>> uploadFiles(

            @RequestPart String clientId,

            @RequestPart String tokenId,

            @RequestPart("files") Flux<FilePart> files
    ) {

        return files
                .flatMap(file -> fileStorageService.save(file, clientId, tokenId))
                .then(Mono.just(
                        ResponseEntity.ok("Files uploaded successfully")
                ));
    }
}

