package org.project.elasticsearchSink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/** Elasticsearch8AsyncSinkSerializer is used to serialize and deserialize an Operation. */
public class Elasticsearch8AsyncSinkSerializer extends AsyncSinkWriterStateSerializer<Operation> {

    @Override
    protected void serializeRequestToStream(Operation request, DataOutputStream out) {
        new OperationSerializer().serialize(request, out);
    }

    @Override
    protected Operation deserializeRequestFromStream(long requestSize, DataInputStream in) {
        return new OperationSerializer().deserialize(requestSize, in);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}