package com.loggingservice.agentbasedservice.controllers;

import tools.jackson.databind.JsonNode;
import com.loggingservice.agentbasedservice.services.LogUploadService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.NoSuchAlgorithmException;

@RestController
@RequestMapping("/logs")
public class LogIngestionController {

    private final LogUploadService logUploadService;

    public LogIngestionController(LogUploadService logUploadService) {
        this.logUploadService = logUploadService;
    }

    @PostMapping(
            value = "/upload",
            consumes = MediaType.APPLICATION_NDJSON_VALUE
    )
    public Mono<ResponseEntity<String>> uploadLogs(
            @RequestHeader(HttpHeaders.AUTHORIZATION) String authorization,
            @RequestBody Flux<JsonNode> logs) throws NoSuchAlgorithmException {

        return logUploadService.uploadLogs(authorization, logs)
                .thenReturn(ResponseEntity.ok("Uploaded"));
    }

}
