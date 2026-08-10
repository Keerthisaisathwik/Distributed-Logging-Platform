package com.loggingservice.agentbasedservice.producer;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.logs-raw}")
    private String topic;

    public Mono<Void> send(JsonNode log) {

        try {

            String json = objectMapper.writeValueAsString(log);

            return Mono
                    .fromFuture(kafkaTemplate.send(topic, json))
                    .then();

        } catch (Exception e) {

            return Mono.error(e);
        }
    }
}