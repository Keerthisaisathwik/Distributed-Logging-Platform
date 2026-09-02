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
import org.project.config.AppConfig;
import org.project.elasticsearchSink.NetworkConfig;
import org.project.model.LogEvent;
import org.project.service.EnrichLogs;
import org.project.service.ParseAndNormalizeLogs;
import org.project.service.ValidationAndDeduplicatingLogs;
import org.project.sink.CassandraSink;
import org.project.sink.ElasticSearchSink;
import org.project.sink.S3Sink;

import java.util.Collections;
import java.util.List;

public class LogProcessingJob {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ParseAndNormalizeLogs parseService;
    private final ValidationAndDeduplicatingLogs validationService;
    private final EnrichLogs enrichService;

    public LogProcessingJob(
            ParseAndNormalizeLogs parseService,
            ValidationAndDeduplicatingLogs validationService,
            EnrichLogs enrichService) {

        this.parseService = parseService;
        this.validationService = validationService;
        this.enrichService = enrichService;
    }

    public void start() throws Exception {
        String rawLogsTopicName = AppConfig.load().getKafkaRawTopic();

        // Multi-broker string from application.properties: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        String bootstrapServers = AppConfig.load().getKafkaBootstrapServers();

        try {
            streamConsumer(rawLogsTopicName, bootstrapServers);
        } catch (Exception e) {
            System.err.println("Error running LogProcessingJob: " + e.getMessage());
            throw e;
        }
    }

    public void streamConsumer(String rawLogsTopicName, String bootstrapServers) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink Checkpoint externalized parameters
        long checkpointInterval = AppConfig.load().getCheckpointIntervalMs();
        environment.enableCheckpointing(checkpointInterval);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(AppConfig.load().getCheckpointTimeoutMs());
        checkpointConfig.setMinPauseBetweenCheckpoints(AppConfig.load().getMinPauseBetweenCheckpointsMs());
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        environment.configure(
                new Configuration()
                        .set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                                AppConfig.load().getCheckpointDir())
        );

        String groupId = AppConfig.load().getKafkaGroupId();
        KafkaSource<String> kafkaSource = createStringConsumerForTopic(rawLogsTopicName, bootstrapServers, groupId);

        DataStream<String> stringInputStream = environment.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Process Logs using injected instance services (fixed duplicate instantiation)
        DataStream<LogEvent> logEventDataStream = this.parseService.process(stringInputStream);
        DataStream<LogEvent> parseLogEventDataStream = this.validationService.process(logEventDataStream);
        DataStream<LogEvent> logStream = this.enrichService.process(parseLogEventDataStream);

        // Print Stream for debugging
        logStream.print();

        // Elasticsearch Sink Configuration
        String esHost = AppConfig.load().getElasticsearchHost();
        int esPort = AppConfig.load().getElasticsearchPort();

        NetworkConfig networkConfig =
                new NetworkConfig(
                        List.of(new HttpHost(esHost, esPort)),
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

        ElasticSearchSink elasticSink = new ElasticSearchSink(networkConfig);
        logStream.sinkTo(elasticSink);

        // Cassandra Sink Configuration
        CassandraSink cassandraSink = new CassandraSink(
                AppConfig.load().getCassandraContactPoints(),
                AppConfig.load().getCassandraPort(),
                AppConfig.load().getCassandraLocalDatacenter(),
                AppConfig.load().getCassandraKeyspace(),
                AppConfig.load().getCassandraMaxBatchLogEvents(),
                AppConfig.load().getCassandraMaxBatchSizeBytes(),
                AppConfig.load().getCassandraFlushIntervalMs(),
                AppConfig.load().getCassandraMaxConcurrentRequests(),
                AppConfig.load().getCassandraMaxRetries()
        );
        logStream.addSink(cassandraSink);

        // S3 Sink Configuration
        S3Sink s3sink = new S3Sink(
                AppConfig.load().getBucket(),
                AppConfig.load().getEndpoint(),
                AppConfig.load().getAccessKey(),
                AppConfig.load().getSecretKey()
        );
        logStream.addSink(s3sink);

        environment.execute("Distributed Logging Platform");
    }

    public KafkaSource<String> createStringConsumerForTopic(String topic, String bootstrapServers, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}