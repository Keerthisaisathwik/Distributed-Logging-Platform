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
import org.project.service.impl.EnrichLogsImpl;
import org.project.service.impl.ParseAndNormalizeLogsImpl;
import org.project.service.impl.ValidationAndDeduplicatingLogsImpl;
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
        String rawLogsTopicName  = AppConfig.load().getInputTopicName();
        String processedLogsTopicName  = AppConfig.load().getProcessedTopicName();
        String failedLogsTopicName  = AppConfig.load().getFailedTopicName();

        String server = "kafka:9092";

        try {
            streamConsumer(rawLogsTopicName, server);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    public void streamConsumer(String rawLogsTopicName, String server) throws Exception {
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
        KafkaSource<String> kafkaSource = createStringConsumerForTopic(rawLogsTopicName, server);
        DataStream<String> stringInputStream = environment.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        //process Logs
        LogProcessingJob job = new LogProcessingJob(
                new ParseAndNormalizeLogsImpl(),
                new ValidationAndDeduplicatingLogsImpl(),
                new EnrichLogsImpl()
        );

        DataStream<LogEvent> logEventDataStream = job.parseService.process(stringInputStream);
        DataStream<LogEvent> parseLogEventDataStream = job.validationService.process(logEventDataStream);
        DataStream<LogEvent> logStream = job.enrichService.process(parseLogEventDataStream);

        //Testing Print Logs
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

        ElasticSearchSink elasticSink = new ElasticSearchSink(networkConfig);
        logStream.sinkTo(elasticSink);

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

        S3Sink s3sink = new S3Sink(
                AppConfig.load().getBucket(),
                AppConfig.load().getEndpoint(),
                AppConfig.load().getAccessKey(),
                AppConfig.load().getSecretKey()
        );
        logStream.addSink(s3sink);

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
