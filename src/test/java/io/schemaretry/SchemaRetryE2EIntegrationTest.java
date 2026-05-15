package io.schemaretry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests using real Kafka and Redis containers.
 * Verifies the full retry lifecycle: consumption -> failure -> retry routing -> successful retry consumption.
 */
@Disabled("Disabled due to Docker API version incompatibility in the environment (requires 1.44+, client uses 1.32)")
@Testcontainers
class SchemaRetryE2EIntegrationTest {

    static {
        System.setProperty("DOCKER_API_VERSION", "1.44");
    }

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private static String bootstrapServers;
    private static String redisUri;

    @BeforeAll
    static void startContainers() {
        bootstrapServers = kafka.getBootstrapServers();
        redisUri = String.format("redis://%s:%d", redis.getHost(), redis.getMappedPort(6379));
    }

    @Test
    void shouldRetryFailedMessageAndThenProcessSuccessfully() throws Exception {
        String mainTopic = "e2e-main";
        String retryTopic = "e2e-main-retry-1s";
        String groupId = "e2e-group";
        String messageId = "msg-e2e-" + System.currentTimeMillis();
        byte[] payload = "e2e-payload".getBytes(StandardCharsets.UTF_8);

        // 1. Create topics
        try (AdminClient admin = AdminClient.create(Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(List.of(
                    new NewTopic(mainTopic, 1, (short) 1),
                    new NewTopic(retryTopic, 1, (short) 1)
            )).all().get();
        }

        // 2. Setup Configuration
        SchemaRetryConfig config = new SchemaRetryConfig();
        config.getKafka().setBootstrapServers(bootstrapServers);
        config.getRedis().setUri(redisUri);
        config.getRetry().getBackoff().setInitialIntervalMs(1000); // 1s
        config.getRetry().setMaxAttempts(3);

        // 3. Setup Collaborators
        RedisStateStore stateStore = new RedisStateStore(redisUri, Duration.ofHours(1), Duration.ofHours(1));
        
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<byte[], Object> producer = new KafkaProducer<>(prodProps);

        RetryOrchestrator orchestrator = new RetryOrchestrator(config, stateStore, producer);

        // 4. Test Logic
        CountDownLatch firstAttemptLatch = new CountDownLatch(1);
        CountDownLatch retryAttemptLatch = new CountDownLatch(1);

        // Consumer for both main and retry topics
        orchestrator.subscribe(mainTopic, groupId, (p, ctx) -> {
            if (ctx.getAttempt() == 0) {
                // First attempt: fail and request retry
                ctx.retry(new RuntimeException("Simulated E2E failure"));
                firstAttemptLatch.countDown();
            }
        });

        orchestrator.subscribe(retryTopic, groupId + "-retry-handler", (p, ctx) -> {
            // This is attempt 1 (the first retry)
            if (ctx.getAttempt() == 1) {
                retryAttemptLatch.countDown();
            }
        });

        // 5. Execution
        producer.send(new ProducerRecord<>(mainTopic, messageId.getBytes(StandardCharsets.UTF_8), payload)).get();

        // 6. Validation
        assertTrue(firstAttemptLatch.await(30, TimeUnit.SECONDS), "Message was not processed from main topic");
        assertTrue(retryAttemptLatch.await(30, TimeUnit.SECONDS), "Message was not processed from retry topic after failure");

        // Cleanup
        orchestrator.close();
        producer.close();
        stateStore.close();
    }
}
