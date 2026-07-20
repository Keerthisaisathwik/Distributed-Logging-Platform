package org.project.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.project.elasticsearchSink.NetworkConfig;
import org.project.model.LogEvent;
import org.project.sink.ElasticSearchSink;
import org.project.util.JsonToLogEventMapper;

import java.util.Collections;
import java.util.List;

public class LogProcessingJob {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void start() throws Exception {
        String inputTopic = "logs.raw";
        String server = "kafka:9092";

        try {
            streamConsumer(inputTopic, server);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    public void streamConsumer(String inputTopic, String server) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(10_000);
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(60_000);
        checkpointConfig.setMinPauseBetweenCheckpoints(5_000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        environment.configure(
                new Configuration()
                        .set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                                "file:///opt/flink/checkpoints")
        );
        KafkaSource<String> kafkaSource = createStringConsumerForTopic(inputTopic, server);
        DataStream<String> stringInputStream = environment.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<LogEvent> logStream = stringInputStream.map(new JsonToLogEventMapper());

        logStream.print();

        NetworkConfig networkConfig =
                new NetworkConfig(
                        List.of(new HttpHost("elasticsearch", 9200)),
                        null,
                        null,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                );

        ElasticSearchSink sink = new ElasticSearchSink(networkConfig);

        logStream.sinkTo(sink);

        environment.execute("Distributed Logging Platform");
    }

    public KafkaSource<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(topic)
                .setGroupId("log-processing-group") // required — no longer optional/commented out
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
