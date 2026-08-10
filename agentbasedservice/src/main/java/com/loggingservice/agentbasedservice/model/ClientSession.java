package com.loggingservice.agentbasedservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientSession {

    private String clientId;
    private String hostName;
    private String expiryTime;
    private String token;
}