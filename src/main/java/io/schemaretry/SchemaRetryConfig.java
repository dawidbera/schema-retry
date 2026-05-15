package io.schemaretry;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Central configuration for the schema-retry library.
 * Supports loading from YAML files or Properties.
 */
public class SchemaRetryConfig {

    @JsonProperty("redis")
    private RedisConfig redis = new RedisConfig();

    @JsonProperty("retry")
    private RetryConfig retry = new RetryConfig();

    @JsonProperty("schema-registry")
    private SchemaRegistryConfig schemaRegistry = new SchemaRegistryConfig();

    @JsonProperty("kafka")
    private KafkaConfig kafka = new KafkaConfig();

    /**
     * Redis-specific configuration settings.
     */
    public static class RedisConfig {
        @JsonProperty("uri")
        private String uri = "redis://localhost:6379";
        @JsonProperty("count-ttl-hours")
        private int countTtlHours = 24;
        @JsonProperty("idempotency-ttl-hours")
        private int idempotencyTtlHours = 1;

        public String getUri() { return uri; }
        public void setUri(String uri) { this.uri = uri; }
        
        public int getCountTtlHours() { return countTtlHours; }
        public void setCountTtlHours(int countTtlHours) { this.countTtlHours = countTtlHours; }
        
        public int getIdempotencyTtlHours() { return idempotencyTtlHours; }
        public void setIdempotencyTtlHours(int idempotencyTtlHours) { this.idempotencyTtlHours = idempotencyTtlHours; }
    }

    /**
     * Global retry configuration settings.
     */
    public static class RetryConfig {
        @JsonProperty("max-attempts")
        private int maxAttempts = 3;
        @JsonProperty("backoff")
        private BackoffConfig backoff = new BackoffConfig();

        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        
        public BackoffConfig getBackoff() { return backoff; }
        public void setBackoff(BackoffConfig backoff) { this.backoff = backoff; }
    }

    /**
     * Backoff-specific configuration settings.
     */
    public static class BackoffConfig {
        @JsonProperty("initial-interval-ms")
        private long initialIntervalMs = 1000;
        @JsonProperty("multiplier")
        private double multiplier = 2.0;
        @JsonProperty("max-interval-ms")
        private long maxIntervalMs = 60000;

        public long getInitialIntervalMs() { return initialIntervalMs; }
        public void setInitialIntervalMs(long initialIntervalMs) { this.initialIntervalMs = initialIntervalMs; }
        
        public double getMultiplier() { return multiplier; }
        public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
        
        public long getMaxIntervalMs() { return maxIntervalMs; }
        public void setMaxIntervalMs(long maxIntervalMs) { this.maxIntervalMs = maxIntervalMs; }
    }

    /**
     * Confluent Schema Registry configuration settings.
     */
    public static class SchemaRegistryConfig {
        @JsonProperty("url")
        private String url = "http://localhost:8081";
        @JsonProperty("capacity")
        private int capacity = 1000;

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }

        /**
         * Creates a SchemaRegistryClient based on this configuration.
         *
         * @return A CachedSchemaRegistryClient instance.
         */
        public SchemaRegistryClient createClient() {
            return new CachedSchemaRegistryClient(url, capacity);
        }
    }

    /**
     * Kafka-specific configuration settings.
     */
    public static class KafkaConfig {
        @JsonProperty("bootstrap-servers")
        private String bootstrapServers = "localhost:9092";

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        
        /**
         * Converts the Kafka configuration to a {@link Properties} object.
         *
         * @param groupId The consumer group ID to use.
         * @return A Properties object populated with Kafka settings.
         */
        public Properties toProperties(String groupId) {
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("group.id", groupId);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            return props;
        }
    }

    /** @return Redis configuration. */
    public RedisConfig getRedis() { return redis; }
    /** @return Retry configuration. */
    public RetryConfig getRetry() { return retry; }
    /** @return Schema Registry configuration. */
    public SchemaRegistryConfig getSchemaRegistry() { return schemaRegistry; }
    /** @return Kafka configuration. */
    public KafkaConfig getKafka() { return kafka; }

    /**
     * Loads configuration from a YAML file.
     * @param file The YAML file.
     * @return A SchemaRetryConfig instance.
     * @throws IOException If loading fails.
     */
    public static SchemaRetryConfig load(File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(file, SchemaRetryConfig.class);
    }

    /**
     * Loads configuration from an input stream (e.g., from classpath).
     * @param is The input stream.
     * @return A SchemaRetryConfig instance.
     * @throws IOException If loading fails.
     */
    public static SchemaRetryConfig load(InputStream is) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(is, SchemaRetryConfig.class);
    }
}
