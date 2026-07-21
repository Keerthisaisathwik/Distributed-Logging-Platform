package com.project.file_ingestion_service.controller;

import com.project.file_ingestion_service.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/logs")
public class LogReceiverController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @PostMapping
    public ResponseEntity<Void> receive(@RequestBody List<Map<String, Object>> logs) {

        System.out.println("Received " + logs.size() + " logs");

        logs.forEach(log -> {
            log.remove("date");
            System.out.println(log);
            kafkaProducer.send(log);
        });
        return ResponseEntity.ok().build();
    }
}

