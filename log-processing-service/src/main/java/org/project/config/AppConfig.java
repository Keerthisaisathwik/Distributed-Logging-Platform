package org.project.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;
    private final String kafkaRawTopic;
    private final String kafkaProcessedTopic;
    private final String kafkaFailedTopic;

    private final long checkpointIntervalMs;
    private final long checkpointTimeoutMs;
    private final long minPauseBetweenCheckpointsMs;
    private final String checkpointDir;

    private final String awsRegion;
    private final String s3Bucket;

    private final String elasticsearchHost;
    private final int elasticsearchPort;

    private final String cassandraContactPoints;
    private final int cassandraPort;
    private final String cassandraLocalDatacenter;
    private final String cassandraKeyspace;

    private final int cassandraMaxBatchLogEvents;
    private final long cassandraMaxBatchSizeBytes;
    private final long cassandraFlushIntervalMs;
    private final int cassandraMaxConcurrentRequests;
    private final int cassandraMaxRetries;

    private final String bucket;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;

    private final int parallelism;

    private AppConfig(Properties properties) {
        this.kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers");
        this.kafkaGroupId = properties.getProperty("kafka.group.id");
        this.kafkaRawTopic = properties.getProperty("kafka.raw.topic");
        this.kafkaProcessedTopic = properties.getProperty("kafka.processed.topic");
        this.kafkaFailedTopic = properties.getProperty("kafka.failed.topic");

        this.checkpointIntervalMs = Long.parseLong(properties.getProperty("flink.checkpoint.interval.ms"));
        this.checkpointTimeoutMs = Long.parseLong(properties.getProperty("flink.checkpoint.timeout.ms"));
        this.minPauseBetweenCheckpointsMs = Long.parseLong(properties.getProperty("flink.checkpoint.min.pause.ms"));
        this.checkpointDir = properties.getProperty("flink.checkpoint.directory");

        this.awsRegion = properties.getProperty("aws.region");
        this.s3Bucket = properties.getProperty("s3.bucket");

        this.elasticsearchHost = properties.getProperty("elasticsearch.host");
        this.elasticsearchPort = Integer.parseInt(properties.getProperty("elasticsearch.port"));

        this.cassandraContactPoints = properties.getProperty("cassandra.contact-points");
        this.cassandraPort = Integer.parseInt(properties.getProperty("cassandra.port"));
        this.cassandraLocalDatacenter = properties.getProperty("cassandra.local-datacenter");
        this.cassandraKeyspace = properties.getProperty("cassandra.keyspace");

        this.cassandraMaxBatchLogEvents = Integer.parseInt(properties.getProperty("cassandra.max-batch-log-events"));
        this.cassandraMaxBatchSizeBytes = Long.parseLong(properties.getProperty("cassandra.max-batch-size-bytes"));
        this.cassandraFlushIntervalMs = Long.parseLong(properties.getProperty("cassandra.flush-interval-ms"));
        this.cassandraMaxConcurrentRequests = Integer.parseInt(properties.getProperty("cassandra.max-concurrent-requests"));
        this.cassandraMaxRetries = Integer.parseInt(properties.getProperty("cassandra.max-retries"));

        this.bucket = properties.getProperty("s3.bucket");
        this.endpoint = properties.getProperty("s3.endpoint");
        this.accessKey = properties.getProperty("s3.access-key");
        this.secretKey = properties.getProperty("s3.secret-key");

        this.parallelism = Integer.parseInt(properties.getProperty("parallelism"));
    }

    public static AppConfig load() {
        Properties properties = new Properties();

        try (InputStream input = AppConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException("application.properties not found");
            }

            properties.load(input);
            return new AppConfig(properties);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getKafkaRawTopic() {
        return kafkaRawTopic;
    }

    public String getKafkaProcessedTopic() {
        return kafkaProcessedTopic;
    }

    public String getKafkaFailedTopic() {
        return kafkaFailedTopic;
    }

    public Long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public Long getCheckpointTimeoutMs() {
        return checkpointTimeoutMs;
    }

    public Long getMinPauseBetweenCheckpointsMs() {
        return minPauseBetweenCheckpointsMs;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getElasticsearchHost() {
        return elasticsearchHost;
    }

    public int getElasticsearchPort() {
        return elasticsearchPort;
    }

    public String getCassandraContactPoints() {
        return cassandraContactPoints;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public String getCassandraLocalDatacenter() {
        return cassandraLocalDatacenter;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    public int getCassandraMaxBatchLogEvents() {
        return cassandraMaxBatchLogEvents;
    }

    public long getCassandraMaxBatchSizeBytes() {
        return cassandraMaxBatchSizeBytes;
    }

    public long getCassandraFlushIntervalMs() {
        return cassandraFlushIntervalMs;
    }

    public int getCassandraMaxConcurrentRequests() {
        return cassandraMaxConcurrentRequests;
    }

    public int getCassandraMaxRetries() {
        return cassandraMaxRetries;
    }

    public String getBucket() {
        return bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public int getParallelism() {
        return parallelism;
    }
}