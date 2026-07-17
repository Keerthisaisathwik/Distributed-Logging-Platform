package org.project.elasticsearchSink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/** OperationSerializer is responsible for serialization and deserialization of an Operation. */
public class OperationSerializer {
    private final Kryo kryo = new Kryo();

    public OperationSerializer() {
        kryo.setRegistrationRequired(false);
        DefaultInstantiatorStrategy instantiatorStrategy = new DefaultInstantiatorStrategy();
        instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setInstantiatorStrategy(instantiatorStrategy);
        // Use TCCL so Kryo can resolve classes that live in the user-code
        // ClassLoader, not the system AppClassLoader.
        kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    }

    public void serialize(Operation request, DataOutputStream out) {
        try (Output output = new Output(out)) {
            kryo.writeObject(output, request);
            output.flush();
        }
    }

    public Operation deserialize(long requestSize, DataInputStream in) {
        try (Input input = new Input(in, (int) requestSize)) {
            return kryo.readObject(input, Operation.class);
        }
    }

    public int size(Operation operation) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeObject(output, operation);
            output.flush();

            return (int) output.total();
        }
    }
}
