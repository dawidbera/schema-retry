package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RetryRouter}.
 */
@ExtendWith(MockitoExtension.class)
class RetryRouterTest {

    @Mock
    private RedisStateStore stateStore;
    @Mock
    private EnvelopeProcessor envelopeProcessor;
    @Mock
    private BackoffStrategy backoffStrategy;
    @Mock
    private KafkaProducer<byte[], Object> producer;

    private RetryRouter retryRouter;

    /**
     * Initializes the test context before each test case.
     */
    @BeforeEach
    void setUp() {
        retryRouter = new RetryRouter(stateStore, envelopeProcessor, backoffStrategy, producer);
    }

    /**
     * Verifies that a message is correctly routed to a retry topic when attempt count 
     * is within the maximum limit.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldRouteToRetryTopic() {
        // Given
        String topic = "orders";
        String msgId = "123";
        byte[] payload = "test-data".getBytes(StandardCharsets.UTF_8);
        Exception ex = new RuntimeException("Database down");
        RetryEnvelope mockEnvelope = mock(RetryEnvelope.class);

        when(stateStore.incrementCount(msgId)).thenReturn(1L);
        when(envelopeProcessor.wrap(eq(payload), eq(42), eq(ex), eq(1))).thenReturn(mockEnvelope);
        when(backoffStrategy.getTargetTopic(topic, 0)).thenReturn("orders-retry-1s");

        // When
        retryRouter.route(topic, msgId, payload, 42, ex, 3);

        // Then
        ArgumentCaptor<ProducerRecord<byte[], Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        
        ProducerRecord<byte[], Object> record = captor.getValue();
        assertEquals("orders-retry-1s", record.topic());
        assertEquals(mockEnvelope, record.value());
        assertArrayEquals(msgId.getBytes(StandardCharsets.UTF_8), record.key());
        
        // Check headers
        assertEquals("java.lang.RuntimeException", new String(record.headers().lastHeader("x-retry-error-class").value()));
        assertEquals("Database down", new String(record.headers().lastHeader("x-retry-error-message").value()));
        assertEquals("1", new String(record.headers().lastHeader("x-retry-attempt").value()));
    }

    /**
     * Verifies that a message is routed to the Dead Letter Queue (DLQ) topic when 
     * the maximum number of retry attempts is exceeded.
     */
    @Test
    @SuppressWarnings("unchecked")
    void shouldRouteToDlqWhenMaxAttemptsExceeded() {
        // Given
        String topic = "orders";
        String msgId = "123";
        Exception ex = new RuntimeException("Fatal");
        
        // 4th attempt when max is 3
        when(stateStore.incrementCount(msgId)).thenReturn(4L);
        when(envelopeProcessor.wrap(any(), anyInt(), any(), anyInt())).thenReturn(mock(RetryEnvelope.class));

        // When
        retryRouter.route(topic, msgId, "data".getBytes(), 42, ex, 3);

        // Then
        ArgumentCaptor<ProducerRecord<byte[], Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        
        ProducerRecord<byte[], Object> record = captor.getValue();
        assertEquals("orders-dlq", record.topic());
        assertEquals("4", new String(record.headers().lastHeader("x-retry-attempt").value()));
        
        // Backoff strategy should NOT be called for DLQ
        verifyNoInteractions(backoffStrategy);
    }
}
