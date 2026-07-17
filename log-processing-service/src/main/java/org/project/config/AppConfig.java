package org.project.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;

    private final String awsRegion;
    private final String s3Bucket;

    private final String elasticsearchHost;
    private final int elasticsearchPort;

    private final int parallelism;

    private AppConfig(Properties properties) {
        this.kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers");
        this.kafkaTopic = properties.getProperty("kafka.topic");

        this.awsRegion = properties.getProperty("aws.region");
        this.s3Bucket = properties.getProperty("s3.bucket");

        this.elasticsearchHost = properties.getProperty("elasticsearch.host");
        this.elasticsearchPort = Integer.parseInt(properties.getProperty("elasticsearch.port"));

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

    public String getKafkaTopic() {
        return kafkaTopic;
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

    public int getParallelism() {
        return parallelism;
    }
}
