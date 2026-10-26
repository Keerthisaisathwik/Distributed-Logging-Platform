package com.project.file_ingestion_service.controller;

import com.project.file_ingestion_service.producer.KafkaProducer;
import com.project.file_ingestion_service.services.FileCompletionTracker;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/logs")
@RequiredArgsConstructor
public class LogReceiverController {

    private final KafkaProducer kafkaProducer;
    private final FileCompletionTracker completionTracker;

    @PostMapping
    public Mono<ResponseEntity<Void>> receive(
            @RequestBody List<Map<String, Object>> incoming) {

        List<Map<String, Object>> acceptedLogs = incoming.stream()
                .filter(record -> !"\r".equals(record.get("log")))
                .toList();

        int removedCount = incoming.size() - acceptedLogs.size();

        if (removedCount > 0) {
            log.info("Filtered {} carriage-return-only records", removedCount);
        }

        List<String> sourceFiles = incoming.stream()
                .map(record -> record.get(FileCompletionTracker.SOURCE_FILE))
                .filter(value -> value != null)
                .map(Object::toString)
                .distinct()
                .toList();

        return kafkaProducer.sendBatch(acceptedLogs)
                .then(
                        Mono.fromRunnable(() -> {
                            try {
                                // Include filtered records because they have
                                // been intentionally accounted for.
                                completionTracker.markProcessed(incoming);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }).subscribeOn(Schedulers.boundedElastic())
                )
                .thenReturn(ResponseEntity.ok().<Void>build())
                .onErrorResume(exception -> {
                    log.error(
                            "Batch processing failed. sourceFiles={} time={}",
                            sourceFiles,
                            Instant.now(),
                            exception
                    );

                    if (kafkaProducer.isAtMostOnce()) {
                        return Mono.just(
                                ResponseEntity.ok().<Void>build()
                        );
                    }

                    return Mono.just(
                            ResponseEntity.status(
                                    HttpStatus.SERVICE_UNAVAILABLE
                            ).<Void>build()
                    );
                });
    }
}