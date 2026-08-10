package org.project.service.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.project.model.LogEvent;
import org.project.service.EnrichLogs;

public class EnrichLogsImpl implements EnrichLogs {

    @Override
    public DataStream<LogEvent> process(DataStream<LogEvent> stream) {
        return stream;
    }
}
