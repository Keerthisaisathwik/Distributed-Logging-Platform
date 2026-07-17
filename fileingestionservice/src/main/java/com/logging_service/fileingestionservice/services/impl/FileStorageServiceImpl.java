package com.logging_service.fileingestionservice.services.impl;

import com.logging_service.fileingestionservice.services.FileStorageService;
import org.reactivestreams.Publisher;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Service
public class FileStorageServiceImpl implements FileStorageService {

    private final Path uploadPath = Paths.get("uploads/logs");

    @Override
    public Mono<Void> save(
            FilePart filePart,
            String clientId,
            String tokenId
    ) {

        Path clientFolder = uploadPath.resolve(clientId);

        try {
            Files.createDirectories(clientFolder);
        } catch (IOException e) {
            return Mono.error(e);
        }

        String newName =
                UUID.randomUUID() + "_" + filePart.filename();

        Path destination =
                clientFolder.resolve(newName);

        return filePart.transferTo(destination);
    }
}
