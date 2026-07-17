package org.project.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.project.model.LogEvent;

public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtils() {
    }

    public static String toJson(LogEvent event) throws JsonProcessingException {
        return MAPPER.writeValueAsString(event);
    }

    public static LogEvent fromJson(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, LogEvent.class);
    }
}