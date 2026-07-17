package org.project.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SenderAndRecieverJob {

    public static void main(String[] args) throws Exception {

        String server = "localhost:9092";
        String inputTopic = "testtopic";
        String outputTopic = "testtopic_output";

        streamStringOperation(server, inputTopic, outputTopic);
    }

    public static class StringCapitalizer implements MapFunction<String, String> {
        @Override
        public String map(String data) throws Exception {
            return data.toUpperCase();
        }
    }

    public static void streamStringOperation(String server, String inputTopic, String outputTopic) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = createStringConsumerForTopic(inputTopic, server);
        KafkaSink<String> kafkaSink = createStringProducer(outputTopic, server);

        DataStream<String> stringInputStream = environment.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        stringInputStream.map(new StringCapitalizer()).sinkTo(kafkaSink);

        environment.execute();
    }

    public static KafkaSource<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(topic)
                .setGroupId("kafka-flink-demo-group") // required now — was commented out before
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSink<String> createStringProducer(String topic, String kafkaAddress) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}