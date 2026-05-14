package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RetryConsumer}.
 * Verifies the consumption, processing, and retry routing logic.
 */
@ExtendWith(MockitoExtension.class)
class RetryConsumerTest {

    @Mock
    private RetryRouter retryRouter;
    @Mock
    private EnvelopeProcessor envelopeProcessor;
    @Mock
    private KafkaConsumer<byte[], Object> kafkaConsumer;
    @Mock
    private RedisStateStore stateStore;

    private RetryConsumer<byte[], byte[]> retryConsumer;

    /**
     * Initializes the test environment before each test case.
     */
    @BeforeEach
    void setUp() {
        when(retryRouter.getStateStore()).thenReturn(stateStore);
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, (v, ctx) -> {
            if (new String(v).equals("fail")) {
                throw new RuntimeException("Simulated failure");
            }
        }, 3);
    }

    /**
     * Verifies that a simple message is processed successfully and the circuit breaker status is checked.
     */
    @Test
    void shouldProcessSimpleMessageSuccessfully() {
        // Given
        byte[] payload = "success".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders", 0, 0, "123".getBytes(), payload);
        when(stateStore.getCircuitBreakerStatus("orders")).thenReturn("CLOSED");

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(stateStore).getCircuitBreakerStatus("orders");
        verifyNoMoreInteractions(retryRouter); // This might still fail if I call it multiple times, but let's be more specific.
    }

    /**
     * Verifies that a handler exception triggers the retry routing logic.
     */
    @Test
    void shouldTriggerRetryOnHandlerException() {
        // Given
        byte[] payload = "fail".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders", 0, 0, "123".getBytes(), payload);
        when(stateStore.getCircuitBreakerStatus("orders")).thenReturn("CLOSED");

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(retryRouter).route(eq("orders"), eq("123"), eq(payload), anyInt(), any(RuntimeException.class), eq(3));
    }

    /**
     * Verifies that a message wrapped in a {@link RetryEnvelope} is correctly unwrapped 
     * and the retry context is populated with the correct attempt number.
     */
    @Test
    void shouldUnwrapRetryEnvelope() {
        // Given
        byte[] originalPayload = "data".getBytes(StandardCharsets.UTF_8);
        RetryEnvelope envelope = mock(RetryEnvelope.class);
        when(envelope.getAttempt()).thenReturn(1);
        when(envelope.getSchemaId()).thenReturn(42);
        when(envelopeProcessor.unwrap(envelope)).thenReturn(originalPayload);
        when(stateStore.getCircuitBreakerStatus("orders-retry-1s")).thenReturn("CLOSED");

        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders-retry-1s", 0, 0, "123".getBytes(), envelope);

        AtomicReference<RetryContext> capturedContext = new AtomicReference<>();
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, (v, ctx) -> {
            capturedContext.set(ctx);
        }, 3);
        when(retryRouter.getStateStore()).thenReturn(stateStore); // Need to re-stub because of new instance

        // When
        retryConsumer.processRecord(record);

        // Then
        assertNotNull(capturedContext.get());
        assertEquals(1, capturedContext.get().getAttempt());
        verify(envelopeProcessor).unwrap(envelope);
    }
}
