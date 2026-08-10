package org.project.model;

import java.time.Instant;

public class FailedLogEvent {

    private String rawPayload;
    private String errorMessage;
    private Instant failedAt;

    public FailedLogEvent() {
    }

    public FailedLogEvent(String rawPayload, String errorMessage, Instant failedAt) {
        this.rawPayload = rawPayload;
        this.errorMessage = errorMessage;
        this.failedAt = failedAt;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }
}
