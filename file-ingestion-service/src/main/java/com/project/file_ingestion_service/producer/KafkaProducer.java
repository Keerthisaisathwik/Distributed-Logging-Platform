package com.project.file_ingestion_service.producer;

import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private static final String SOURCE_FILE = "_source_file";
    private static final String SOURCE_OFFSET = "_source_offset";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.logs-raw}")
    private String topic;

    @Value("${ingestion.delivery-mode:AT_LEAST_ONCE}")
    private String deliveryMode;

    @Value("${ingestion.retry.max-retries:5}")
    private long maxRetries;

    @Value("${ingestion.retry.initial-delay-ms:500}")
    private long initialDelayMs;

    @Value("${ingestion.retry.max-delay-seconds:30}")
    private long maxDelaySeconds;

    public boolean isAtMostOnce() {
        return "AT_MOST_ONCE".equalsIgnoreCase(deliveryMode);
    }

    public Mono<Void> sendBatch(List<Map<String, Object>> logs) {
        if (logs.isEmpty()) {
            return Mono.empty();
        }

        // Send concurrently; Kafka's producer batches records internally.
        return Flux.fromIterable(logs)
                .flatMap(this::sendWithPolicy, 32)
                .then();
    }

    private Mono<Void> sendWithPolicy(Map<String, Object> logRecord) {
        String sourceFile = String.valueOf(
                logRecord.getOrDefault(SOURCE_FILE, "unknown")
        );

        Mono<Void> sendAttempt = Mono.defer(() -> {
            try {
                Map<String, Object> payload =
                        new LinkedHashMap<>(logRecord);

                Object offsetValue = payload.get(SOURCE_OFFSET);
                String sourceOffset = offsetValue == null
                        ? null
                        : String.valueOf(offsetValue);

                // These are ingestion metadata, not application log fields.
                payload.remove("date");
                payload.remove(SOURCE_FILE);
                payload.remove(SOURCE_OFFSET);

                String json = objectMapper.writeValueAsString(payload);

                // A deterministic key helps identify the same source record.
                // It does not, by itself, deduplicate Kafka records.
                String messageKey = sourceOffset == null
                        || "unknown".equals(sourceFile)
                        ? null
                        : sourceFile + ":" + sourceOffset;

                return Mono.fromFuture(
                        kafkaTemplate.send(topic, messageKey, json)
                ).then();

            } catch (Exception exception) {
                return Mono.error(exception);
            }
        });

        if (isAtMostOnce() || maxRetries <= 0) {
            return sendAttempt;
        }

        return sendAttempt.retryWhen(
                Retry.backoff(
                                maxRetries,
                                Duration.ofMillis(initialDelayMs)
                        )
                        .maxBackoff(Duration.ofSeconds(maxDelaySeconds))
                        .doBeforeRetry(signal ->
                                log.warn(
                                        "Retry {} for source file {} at {}",
                                        signal.totalRetries() + 1,
                                        sourceFile,
                                        Instant.now()
                                )
                        )
        );
    }
}