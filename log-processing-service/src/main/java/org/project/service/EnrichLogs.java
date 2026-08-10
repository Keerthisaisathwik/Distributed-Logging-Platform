package org.project.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.project.model.LogEvent;

public interface EnrichLogs {
    DataStream<LogEvent> process(DataStream<LogEvent> stream);
}
