package org.project.service.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.project.model.FailedLogEvent;
import org.project.model.LogEvent;
import org.project.service.ParseAndNormalizeLogs;
import org.project.util.JsonToLogEventProcessFunction;
import org.project.util.ParseResult;

public class ParseAndNormalizeLogsImpl implements ParseAndNormalizeLogs {

    private static final OutputTag<FailedLogEvent> FAILED_LOGS_TAG = new OutputTag<FailedLogEvent>("failed-logs") {};

    @Override
    public DataStream<LogEvent> process(DataStream<String> stream) {
        SingleOutputStreamOperator<LogEvent> validLogs = stream
                .process(new JsonToLogEventProcessFunction(FAILED_LOGS_TAG))
                .name("parse-and-normalize-logs")
                .uid("parse-and-normalize-logs");

        validLogs.print("PARSED_LOG");

        DataStream<FailedLogEvent> failedLogs = validLogs.getSideOutput(FAILED_LOGS_TAG);

        //sink(new ParseResult(validLogs, failedLogs).getFailedLogs());
        ParseResult parseResult = new ParseResult(validLogs, failedLogs);

        DataStream<FailedLogEvent> dataStreamFailedLogs = parseResult.getFailedLogs();
        dataStreamFailedLogs.print("FAILED_LOG");

        return parseResult.getValidLogs();
    }
}
