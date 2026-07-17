package org.project.elasticsearchSink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public class Elasticsearch8AsyncSink<InputT> extends AsyncSinkBase<InputT, Operation> {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch8AsyncSink.class);

    @VisibleForTesting protected final NetworkConfig networkConfig;

    protected Elasticsearch8AsyncSink(
            ElementConverter<InputT, Operation> converter,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInByte,
            NetworkConfig networkConfig) {
        super(
                converter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInByte);

        this.networkConfig = networkConfig;
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> createWriter(
            WriterInitContext context) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Operation>> restoreWriter(
            WriterInitContext context, Collection<BufferedRequestState<Operation>> recoveredState) {
        return new Elasticsearch8AsyncWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                networkConfig,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Operation>> getWriterStateSerializer() {
        return new Elasticsearch8AsyncSinkSerializer();
    }
}