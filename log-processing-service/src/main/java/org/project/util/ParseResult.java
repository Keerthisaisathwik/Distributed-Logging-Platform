package org.project.util;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.project.model.FailedLogEvent;
import org.project.model.LogEvent;

public final class ParseResult {

    private final DataStream<LogEvent> validLogs;
    private final DataStream<FailedLogEvent> failedLogs;

    public ParseResult(DataStream<LogEvent> validLogs, DataStream<FailedLogEvent> failedLogs) {
        this.validLogs = validLogs;
        this.failedLogs = failedLogs;
    }

    public DataStream<LogEvent> getValidLogs() {
        return validLogs;
    }

    public DataStream<FailedLogEvent> getFailedLogs() {
        return failedLogs;
    }
}
