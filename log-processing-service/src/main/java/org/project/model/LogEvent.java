package org.project.model;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

public class LogEvent {

    private String logId;
    private String host;
    private Environment env;
    private Instant timestamp;
    private Instant ingestionTimestamp;
    private String service;
    private String instanceId;
    private String namespace;
    private String podName;
    private String traceId;
    private LogLevel severityLevel;
    private String message;
    private List<LogAttribute> attributes;

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Environment getEnv() { return env; }

    public void setEnv(Environment env) { this.env = env; }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Instant getIngestionTimestamp() {
        return timestamp;
    }

    public void setIngestionTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public LogLevel getSeverityLevel() {
        return severityLevel;
    }

    public void setSeverityLevel(LogLevel level) {
        this.severityLevel = level;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<LogAttribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<LogAttribute> attributes) {
        this.attributes = attributes;
    }

    //For getting only date
    public LocalDate getLogDate() {
        return this.timestamp.atZone(ZoneOffset.UTC).toLocalDate();
    }

}
