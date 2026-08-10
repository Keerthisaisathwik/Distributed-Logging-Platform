package com.loggingservice.agentbasedservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientValidationResponse {

    private boolean valid;
    private String clientId;
    private String hostName;
    private String expiryTime;

}