package org.project.service.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.project.model.LogEvent;
import org.project.service.ValidationAndDeduplicatingLogs;

public class ValidationAndDeduplicatingLogsImpl implements ValidationAndDeduplicatingLogs {

    @Override
    public DataStream<LogEvent> process(DataStream<LogEvent> stream) {
        return stream;
    }
}
