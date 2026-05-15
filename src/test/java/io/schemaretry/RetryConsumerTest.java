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
    @Mock
    private AvroSerdeService avroSerdeService;

    private RetryConsumer<byte[], Object> retryConsumer;

    /**
     * Initializes the test environment before each test case.
     */
    @BeforeEach
    void setUp() {
        when(retryRouter.getStateStore()).thenReturn(stateStore);
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, avroSerdeService, (v, ctx) -> {
            if ("fail".equals(v)) {
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
        when(avroSerdeService.extractSchemaId(payload)).thenReturn(-1);
        when(avroSerdeService.deserialize("orders", payload, -1)).thenReturn("success");

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(stateStore).getCircuitBreakerStatus("orders");
        verify(stateStore).checkAndMarkIdempotent("123");
        verify(avroSerdeService).deserialize("orders", payload, -1);
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
        when(avroSerdeService.extractSchemaId(payload)).thenReturn(-1);

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(stateStore).checkAndMarkIdempotent("123");
        // Verify that the handler was NOT called.
        verify(avroSerdeService, never()).deserialize(anyString(), any(), anyInt());
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
        when(avroSerdeService.extractSchemaId(payload)).thenReturn(-1);
        when(avroSerdeService.deserialize("orders", payload, -1)).thenReturn("fail");

        // When
        retryConsumer.processRecord(record);

        // Then
        verify(retryRouter).route(eq("orders"), eq("123"), eq(payload), anyInt(), any(RuntimeException.class), eq(3));
    }

    /**
     * Verifies that a message wrapped in a {@link RetryEnvelope} is correctly unwrapped,
     * deserialized, and the retry context is populated with the correct attempt number.
     */
    @Test
    void shouldUnwrapAndDeserializeRetryEnvelope() {
        // Given
        byte[] originalPayload = "data".getBytes(StandardCharsets.UTF_8);
        RetryEnvelope envelope = mock(RetryEnvelope.class);
        when(envelope.getAttempt()).thenReturn(1);
        when(envelope.getSchemaId()).thenReturn(42);
        when(envelopeProcessor.unwrap(envelope)).thenReturn(originalPayload);
        when(stateStore.getCircuitBreakerStatus("orders-retry-1s")).thenReturn("CLOSED");
        when(stateStore.checkAndMarkIdempotent("123")).thenReturn(false);
        when(avroSerdeService.deserialize("orders-retry-1s", originalPayload, 42)).thenReturn("deserialized-data");

        ConsumerRecord<byte[], Object> record = new ConsumerRecord<>("orders-retry-1s", 0, 0, "123".getBytes(), envelope);

        AtomicReference<Object> capturedValue = new AtomicReference<>();
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, avroSerdeService, (v, ctx) -> {
            capturedValue.set(v);
        }, 3);
        when(retryRouter.getStateStore()).thenReturn(stateStore);

        // When
        retryConsumer.processRecord(record);

        // Then
        assertEquals("deserialized-data", capturedValue.get());
        verify(avroSerdeService).deserialize("orders-retry-1s", originalPayload, 42);
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
        when(avroSerdeService.extractSchemaId(payload)).thenReturn(-1);
        when(avroSerdeService.deserialize(eq("orders"), eq(payload), anyInt())).thenReturn("mdc-test");

        AtomicReference<String> capturedMdcId = new AtomicReference<>();
        AtomicReference<String> capturedMdcTopic = new AtomicReference<>();
        
        retryConsumer = new RetryConsumer<>(kafkaConsumer, retryRouter, envelopeProcessor, avroSerdeService, (v, ctx) -> {
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
