package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the Schema Retry library.
 * Verifies the end-to-end flow of message consumption, retry routing, and state persistence using mocks.
 */
class SchemaRetryIntegrationTest {

    private static final String MAIN_TOPIC = "orders";
    private static final String RETRY_TOPIC = "orders-retry-1s";

    private MockConsumer<byte[], Object> mockConsumer;
    private MockProducer<byte[], Object> mockProducer;
    private RedisStateStore stateStore;
    private RetryRouter retryRouter;
    private EnvelopeProcessor envelopeProcessor;

    /**
     * Initializes the test environment with mocks for Kafka and Redis.
     */
    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        // Using a dummy serializer for Object to avoid type inference issues with ByteArraySerializer
        Serializer<Object> valueSerializer = (topic, data) -> null;
        mockProducer = new MockProducer<>(true, new ByteArraySerializer(), valueSerializer);
        
        stateStore = mock(RedisStateStore.class);
        envelopeProcessor = new EnvelopeProcessor();
        BackoffStrategy backoffStrategy = new ExponentialBackoffStrategy(java.time.Duration.ofSeconds(1), 2.0, java.time.Duration.ofMinutes(1));
        
        retryRouter = new RetryRouter(stateStore, envelopeProcessor, backoffStrategy, mockProducer);
    }

    /**
     * Tests the full lifecycle of a message that fails processing and is routed to a retry topic.
     * Verifies that:
     * 1. The message is consumed from the main topic.
     * 2. The Redis state store is updated (increment count).
     * 3. A new message (wrapped in a RetryEnvelope) is produced to the correct retry topic.
     *
     * @throws Exception if any error occurs during the test execution.
     */
    @Test
    void shouldHandleFullRetryLifecycleWithMocks() throws Exception {
        String messageId = "msg-999";
        byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);

        // Given
        when(stateStore.incrementCount(messageId)).thenReturn(1L);
        when(stateStore.getCircuitBreakerStatus(anyString())).thenReturn("CLOSED");

        CountDownLatch latch = new CountDownLatch(1);

        // We use a specialized RetryConsumer that uses our mockConsumer
        try (RetryConsumer<byte[], byte[]> consumer = new RetryConsumer<>(
                mockConsumer, 
                retryRouter, 
                envelopeProcessor, 
                (p, ctx) -> {
                    ctx.retry(new RuntimeException("Simulated processing error"));
                    latch.countDown();
                }, 
                3)) {

            // Simulate Kafka behavior
            mockConsumer.updateBeginningOffsets(new HashMap<>() {{
                put(new TopicPartition(MAIN_TOPIC, 0), 0L);
            }});
            
            // Add partitions to the topic so subscribe can find them
            mockConsumer.updatePartitions(MAIN_TOPIC, Collections.singletonList(
                    new org.apache.kafka.common.PartitionInfo(MAIN_TOPIC, 0, null, null, null)
            ));

            consumer.start(MAIN_TOPIC);

            // For MockConsumer, trigger a rebalance to assign partitions
            mockConsumer.rebalance(Collections.singletonList(new TopicPartition(MAIN_TOPIC, 0)));
            
            // Add record to mock consumer
            mockConsumer.addRecord(new ConsumerRecord<>(MAIN_TOPIC, 0, 0, messageId.getBytes(StandardCharsets.UTF_8), payload));

            // When
            // The consumer loop in RetryConsumer will poll this record
            
            // Then
            assertTrue(latch.await(10, TimeUnit.SECONDS), "Message was not processed in time");
            
            // Verify Redis was called
            verify(stateStore).incrementCount(messageId);

            // Verify message was produced to retry topic
            List<ProducerRecord<byte[], Object>> history = mockProducer.history();
            assertFalse(history.isEmpty(), "No records produced");
            
            ProducerRecord<byte[], Object> record = history.get(0);
            assertEquals(RETRY_TOPIC, record.topic());
            assertEquals(messageId, new String(record.key()));
            assertTrue(record.value() instanceof RetryEnvelope);
            
            RetryEnvelope envelope = (RetryEnvelope) record.value();
            assertEquals(1, envelope.getAttempt());
            assertEquals("Simulated processing error", envelope.getErrorMessage());
        }
    }

    /**
     * Verifies that messages already processed are skipped based on idempotency check.
     */
    @Test
    void shouldSkipAlreadyProcessedMessage() throws Exception {
        String messageId = "msg-dup";
        byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);

        // Given
        when(stateStore.getCircuitBreakerStatus(anyString())).thenReturn("CLOSED");
        // Mark as already processed
        when(stateStore.checkAndMarkIdempotent(messageId)).thenReturn(true);

        CountDownLatch latch = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try (RetryConsumer<byte[], byte[]> consumer = new RetryConsumer<>(
                mockConsumer, 
                retryRouter, 
                envelopeProcessor, 
                (p, ctx) -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                }, 
                3)) {

            mockConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(MAIN_TOPIC, 0), 0L));
            mockConsumer.updatePartitions(MAIN_TOPIC, Collections.singletonList(
                    new org.apache.kafka.common.PartitionInfo(MAIN_TOPIC, 0, null, null, null)
            ));

            consumer.start(MAIN_TOPIC);
            mockConsumer.rebalance(Collections.singletonList(new TopicPartition(MAIN_TOPIC, 0)));
            mockConsumer.addRecord(new ConsumerRecord<>(MAIN_TOPIC, 0, 0, messageId.getBytes(StandardCharsets.UTF_8), payload));

            // When
            // Wait a bit to ensure it had time to process (or skip)
            assertFalse(latch.await(2, TimeUnit.SECONDS), "Handler should NOT have been called");
            
            // Then
            assertEquals(0, callCount.get(), "Handler was invoked for an idempotent message");
            verify(stateStore).checkAndMarkIdempotent(messageId);
        }
    }

    /**
     * Verifies that messages are re-routed to a retry topic when the circuit breaker is OPEN.
     */
    @Test
    void shouldRouteToRetryWhenCircuitIsOpen() throws Exception {
        String messageId = "msg-cb";
        byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);

        // Given
        when(stateStore.getCircuitBreakerStatus(MAIN_TOPIC)).thenReturn("OPEN");

        CountDownLatch latch = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try (RetryConsumer<byte[], byte[]> consumer = new RetryConsumer<>(
                mockConsumer, 
                retryRouter, 
                envelopeProcessor, 
                (p, ctx) -> {
                    callCount.incrementAndGet();
                    latch.countDown();
                }, 
                3)) {

            mockConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(MAIN_TOPIC, 0), 0L));
            mockConsumer.updatePartitions(MAIN_TOPIC, Collections.singletonList(
                    new org.apache.kafka.common.PartitionInfo(MAIN_TOPIC, 0, null, null, null)
            ));

            consumer.start(MAIN_TOPIC);
            mockConsumer.rebalance(Collections.singletonList(new TopicPartition(MAIN_TOPIC, 0)));
            mockConsumer.addRecord(new ConsumerRecord<>(MAIN_TOPIC, 0, 0, messageId.getBytes(StandardCharsets.UTF_8), payload));

            // When
            // Wait for processing
            Thread.sleep(1000); 
            
            // Then
            assertEquals(0, callCount.get(), "Handler was invoked while circuit is OPEN");
            verify(stateStore).getCircuitBreakerStatus(MAIN_TOPIC);
            
            // Verify message was re-routed to retry topic
            List<ProducerRecord<byte[], Object>> history = mockProducer.history();
            assertFalse(history.isEmpty(), "No records produced (should have been re-routed)");
            assertEquals(RETRY_TOPIC, history.get(0).topic());
            
            // Verify attempt count was NOT incremented (remains 0 in this case)
            RetryEnvelope envelope = (RetryEnvelope) history.get(0).value();
            assertEquals(0, envelope.getAttempt());
            assertTrue(envelope.getErrorMessage().toString().contains("Circuit Breaker OPEN"));
        }
    }
}
