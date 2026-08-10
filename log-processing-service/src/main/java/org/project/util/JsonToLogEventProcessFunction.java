package org.project.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.project.model.FailedLogEvent;
import org.project.model.LogAttribute;
import org.project.model.LogEvent;
import org.project.model.LogLevel;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class JsonToLogEventProcessFunction extends ProcessFunction<String, LogEvent> {

    private final OutputTag<FailedLogEvent> failedLogsTag;
    private transient ObjectMapper objectMapper;
    String podName = System.getenv("POD_NAME");

    public JsonToLogEventProcessFunction(OutputTag<FailedLogEvent> failedLogsTag) {
        this.failedLogsTag = failedLogsTag;
    }

    @Override
    public void open(OpenContext openContext) {
        objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public void processElement(String rawLog, Context ctx, Collector<LogEvent> out) {
        try {
            if (rawLog == null || rawLog.isBlank()) {
                throw new IllegalArgumentException("Log record is empty");
            }

            JsonNode json = objectMapper.readTree(rawLog);

            if (json == null || !json.isObject()) {
                throw new IllegalArgumentException(
                        "Log record must be a valid JSON object"
                );
            }

            Instant ingestionTimestamp = Instant.now();

            LogEvent logEvent = new LogEvent();

            logEvent.setLogId(getText(json, "logId"));

            logEvent.setHost(getText(json, "host"));

            logEvent.setTimestamp(parseTimestamp(
                    firstNonBlank(getText(json, "timestamp")),
                    ingestionTimestamp
            ));

            logEvent.setIngestionTimestamp(ingestionTimestamp);

            logEvent.setService(firstNonBlank(
                    getText(json, "service"),
                    "unknown-service"
            ));

            logEvent.setInstanceId(getText(json, "instanceId"));

            logEvent.setNamespace(getText(json, "namespace"));

            logEvent.setPodName(getText(json, "podName"));

            logEvent.setTraceId(getText(json, "traceId"));

            logEvent.setSeverityLevel(parseLogLevel(getText(json, "severityLevel")));

            logEvent.setMessage(getText(json, "message"));

            List<LogAttribute> attributes = parseAttributes(json);

//            int attributesSize = Math.min(10, attributes.size());
//            for(int i=0;i<=attributesSize;i++){
//                attributes.get(i).setLogID(logEvent.getLogId());
//            }

            for (LogAttribute attribute : attributes) {
                attribute.setLogId(logEvent.getLogId());
            }

            logEvent.setAttributes(attributes);

            validate(logEvent);

            out.collect(logEvent);

        } catch (Exception exception) {
            String errorMessage = exception.getMessage();

            if (errorMessage == null || errorMessage.isBlank()) {
                errorMessage = exception.getClass().getSimpleName();
            }

            ctx.output(
                    failedLogsTag,
                    new FailedLogEvent(
                            rawLog,
                            errorMessage,
                            Instant.now()
                    )
            );
        }
    }

    private String getText(JsonNode json, String fieldName) {
        JsonNode value = json.get(fieldName);

        if (value == null || value.isNull()) {
            return null;
        }

        if (value.isTextual()
                || value.isNumber()
                || value.isBoolean()) {

            String text = value.asText();

            return text == null || text.isBlank()
                    ? null
                    : text.trim();
        }

        return null;
    }

    private String getNestedText(
            JsonNode json,
            String... path) {

        JsonNode current = json;

        for (String field : path) {
            if (current == null || current.isNull()) {
                return null;
            }

            current = current.get(field);
        }

        if (current == null || current.isNull()) {
            return null;
        }

        String value = current.asText(null);

        return value == null || value.isBlank()
                ? null
                : value.trim();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }

        return null;
    }

    private Instant parseTimestamp(
            String timestamp,
            Instant defaultTimestamp) {

        if (timestamp == null || timestamp.isBlank()) {
            return defaultTimestamp;
        }

        String value = timestamp.trim();

        /*
         * Epoch timestamp support:
         *
         * 10 digits  -> seconds
         * 13 digits  -> milliseconds
         * 16 digits  -> microseconds
         * 19 digits  -> nanoseconds
         */
        if (value.matches("-?\\d+")) {
            try {
                long epochValue = Long.parseLong(value);

                int digitCount = value.startsWith("-")
                        ? value.length() - 1
                        : value.length();

                if (digitCount <= 10) {
                    return Instant.ofEpochSecond(epochValue);
                }

                if (digitCount <= 13) {
                    return Instant.ofEpochMilli(epochValue);
                }

                if (digitCount <= 16) {
                    long seconds = epochValue / 1_000_000;
                    long microseconds = epochValue % 1_000_000;

                    return Instant.ofEpochSecond(
                            seconds,
                            microseconds * 1_000
                    );
                }

                long seconds = epochValue / 1_000_000_000;
                long nanoseconds = epochValue % 1_000_000_000;

                return Instant.ofEpochSecond(
                        seconds,
                        nanoseconds
                );

            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(
                        "Invalid numeric timestamp: " + value,
                        exception
                );
            }
        }

        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ignored) {
            // Try the next format.
        }

        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException ignored) {
            // Try local date-time formats.
        }

        DateTimeFormatter[] formatters = {
                DateTimeFormatter.ISO_LOCAL_DATE_TIME,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
        };

        for (DateTimeFormatter formatter : formatters) {
            try {
                /*
                 * Logs without a timezone are treated as UTC.
                 * Change ZoneOffset.UTC if your platform uses another
                 * default timezone.
                 */
                LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);

                return localDateTime.toInstant(ZoneOffset.UTC);

            } catch (DateTimeParseException ignored) {
                // Try the next formatter.
            }
        }

        throw new IllegalArgumentException(
                "Unsupported timestamp format: " + value
        );
    }

    private LogLevel parseLogLevel(String rawLevel) {
        if (rawLevel == null || rawLevel.isBlank()) {
            return LogLevel.INFO;
        }

        String normalized = rawLevel
                .trim()
                .toUpperCase(Locale.ROOT);

        normalized = switch (normalized) {
            case "TRACE", "TRC" -> "TRACE";
            case "DEBUG", "DBG" -> "DEBUG";
            case "INFO", "INFORMATION", "INFORMATIONAL" -> "INFO";
            case "WARN", "WARNING" -> "WARN";
            case "ERROR", "ERR", "SEVERE" -> "ERROR";
            case "FATAL", "CRITICAL", "CRIT" -> "FATAL";
            default -> normalized;
        };

        try {
            return LogLevel.valueOf(normalized);
        } catch (IllegalArgumentException exception) {
            /*
             * You can return INFO here instead if an unknown log level
             * should not fail the complete record.
             */
            throw new IllegalArgumentException(
                    "Unsupported log level: " + rawLevel,
                    exception
            );
        }
    }

    private List<LogAttribute> parseAttributes(JsonNode json, String logId) {
        JsonNode attributesNode = json.get("attributes");

        if (attributesNode == null || attributesNode.isNull()) {
            return Collections.emptyList();
        }

        if (!attributesNode.isArray()) {
            throw new IllegalArgumentException(
                    "'attributes' must be a JSON array"
            );
        }

        //traverse
        Iterator<Map.Entry<String, JsonNode>> fields = attributesNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            LogAttribute logAttribute = new LogAttribute();
            logAttribute.setLogId(logId);
            logAttribute.setId(podName + "" + Instant.now() + "" + UUID.randomUUID().toString().substring(0,10));
            String key = field.getKey();
            JsonNode value = field.getValue();

            System.out.println("Key: " + key);
            System.out.println("Value: " + value);
        }

        return objectMapper.convertValue(
                attributesNode,
                new TypeReference<List<LogAttribute>>() {
                }
        );
    }

    private void validate(LogEvent logEvent) {
        if (logEvent.getTimestamp() == null) {
            logEvent.setTimestamp(Instant.now());
        }

        if (logEvent.getSeverityLevel() == null) {
            logEvent.setSeverityLevel(LogLevel.INFO);
        }

        if (logEvent.getMessage() == null || logEvent.getMessage().isBlank()) {
            throw new IllegalArgumentException(
                    "Log message is missing"
            );
        }
    }
}
