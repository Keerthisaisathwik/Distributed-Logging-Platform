package org.project.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.project.model.LogEvent;

public class JsonToLogEventMapper implements MapFunction<String, LogEvent> {

    private transient ObjectMapper mapper;

    @Override
    public LogEvent map(String value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }

        try {
            return mapper.readValue(value, LogEvent.class);
        } catch (Exception e) {
            System.out.println("Failed JSON:");
            System.out.println(value);

            e.printStackTrace();

            throw e;
        }
    }
}
