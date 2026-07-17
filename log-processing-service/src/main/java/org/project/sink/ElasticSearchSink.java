package org.project.sink;

import org.project.elasticsearchSink.Elasticsearch8AsyncSink;
import org.project.elasticsearchSink.NetworkConfig;
import org.project.elasticsearchSink.Operation;
import org.project.model.LogEvent;

import org.apache.flink.connector.base.sink.writer.ElementConverter;

public class ElasticSearchSink extends Elasticsearch8AsyncSink<LogEvent> {

    public ElasticSearchSink(NetworkConfig networkConfig) {
        super(
                new LogEventConverter(),    // Converts LogEvent -> Operation
                1000,                       // Max batch size
                5,                          // Max in-flight requests
                10000,                      // Max buffered requests
                5 * 1024 * 1024,            // 5 MB batch size
                2000,                       // Flush after 2 seconds
                1024 * 1024,                // Max record size (1 MB)
                networkConfig
        );
    }

    private static class LogEventConverter
            implements ElementConverter<LogEvent, Operation> {

        @Override
        public Operation apply(LogEvent log,
                               org.apache.flink.api.connector.sink2.SinkWriter.Context context) {

            return new Operation(
                    co.elastic.clients.elasticsearch.core.bulk.IndexOperation
                            .of(i -> i
                                    .index("logs")
                                    .document(log))
            );
        }
    }
}