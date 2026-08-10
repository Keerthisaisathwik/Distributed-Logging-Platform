package com.loggingservice.agentbasedservice.services;

import com.loggingservice.agentbasedservice.model.ClientSession;
import com.loggingservice.agentbasedservice.producer.KafkaProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class LogUploadService {

    private static final long MAX_SIZE = 5 * 1024 * 1024;

    private final JwtService jwtService;
    private final RedisService redisService;
    private final KafkaProducer kafkaProducer;


    public Mono<Void> uploadLogs(String authorization,
                                 Flux<JsonNode> logs) {

        String token = authorization.substring(7);

        return jwtService.isValid(token)
                .flatMap(isValid -> {

                    if (!isValid) {
                        return Mono.error(
                                new ResponseStatusException(
                                        HttpStatus.UNAUTHORIZED,
                                        "Invalid token"
                                ));
                    }

                    return redisService.get(token)
                            .cast(ClientSession.class)
                            .flatMap((ClientSession session) -> {

                                AtomicLong size = new AtomicLong();

                                return logs
                                        .filter(log ->
                                                log.has("message") &&
                                                        !log.get("message").isNull())
                                        .flatMap(log -> {

                                            ((ObjectNode) log).put("hostName", session.getHostName());

                                            ((ObjectNode) log).put(
                                                    "ingestionTimestamp",
                                                    Instant.now().toString());
                                            String eventId = UUID.randomUUID().toString();
                                            ((ObjectNode) log).put(
                                                    "eventId",
                                                    eventId);

                                            return kafkaProducer.send(log);

                                        })
                                        .then();
                            });
                });
    }
}
