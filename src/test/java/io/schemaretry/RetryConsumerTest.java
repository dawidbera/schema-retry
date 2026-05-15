package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.slf4j.MDC;
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
        when(stateStore.checkAndMarkIdempotent("123")).thenReturn(false);

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(stateStore).getCircuitBreakerStatus("orders");
        verify(stateStore).checkAndMarkIdempotent("123");
        verifyNoMoreInteractions(retryRouter);
    }

    /**
     * Verifies that a message is skipped if it has already been processed (idempotency).
     */
    @Test
    void shouldSkipMessageIfAlreadyProcessed() {
        // Given
        byte[] payload = "success".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders", 0, 0, "123".getBytes(), payload);
        when(stateStore.getCircuitBreakerStatus("orders")).thenReturn("CLOSED");
        when(stateStore.checkAndMarkIdempotent("123")).thenReturn(true);

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(stateStore).checkAndMarkIdempotent("123");
        // Verify that the handler was NOT called. Since handler is mocked by the test's setup logic, 
        // we check that retryRouter (which would be called on failure/success if logic continued) is not interacted with further.
        verifyNoMoreInteractions(retryRouter);
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
        when(stateStore.checkAndMarkIdempotent("123")).thenReturn(false);

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
        when(stateStore.checkAndMarkIdempotent("123")).thenReturn(false);

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

    /**
     * Verifies that MDC context is correctly set and cleared during record processing.
     */
    @Test
    void shouldPropagateMdcContext() {
        // Given
        byte[] payload = "mdc-test".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders", 0, 0, "msg-1".getBytes(), payload);
        when(stateStore.getCircuitBreakerStatus("orders")).thenReturn("CLOSED");
        when(stateStore.checkAndMarkIdempotent("msg-1")).thenReturn(false);

        AtomicReference<String> capturedMdcId = new AtomicReference<>();
        AtomicReference<String> capturedMdcTopic = new AtomicReference<>();
        
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, (v, ctx) -> {
            capturedMdcId.set(MDC.get("messageId"));
            capturedMdcTopic.set(MDC.get("topic"));
        }, 3);
        when(retryRouter.getStateStore()).thenReturn(stateStore);

        // When
        retryConsumer.processRecord(record);

        // Then
        assertEquals("msg-1", capturedMdcId.get());
        assertEquals("orders", capturedMdcTopic.get());
        assertNull(MDC.get("messageId"), "MDC should be cleared after processing");
    }
}
