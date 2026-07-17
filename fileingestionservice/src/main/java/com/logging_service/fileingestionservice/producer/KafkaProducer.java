package com.logging_service.fileingestionservice.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.logs-raw}")
    private String topic;

    public void send(Map<String, Object> log) {

        try {
            String json = objectMapper.writeValueAsString(log);

            kafkaTemplate.send(topic, json);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert log to JSON", e);
        }
    }
}