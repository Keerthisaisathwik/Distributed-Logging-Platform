package org.project.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.project.model.LogEvent;

public interface ParseAndNormalizeLogs {
    DataStream<LogEvent> process(DataStream<String> stream);
}
