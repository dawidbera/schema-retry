package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link EnvelopeProcessor}.
 */
class EnvelopeProcessorTest {

    private final EnvelopeProcessor processor = new EnvelopeProcessor();

    /**
     * Verifies that original payload data, schema IDs, and metadata are correctly wrapped 
     * into a RetryEnvelope and can be successfully unwrapped.
     */
    @Test
    void shouldWrapAndUnwrapPayload() {
        // Given
        byte[] originalData = "test-order-data".getBytes(StandardCharsets.UTF_8);
        int schemaId = 42;
        Exception error = new RuntimeException("Service unavailable");
        int attempt = 2;

        // When
        RetryEnvelope envelope = processor.wrap(originalData, schemaId, error, attempt);

        // Then
        assertNotNull(envelope);
        assertEquals(schemaId, envelope.getSchemaId());
        assertEquals("Service unavailable", envelope.getErrorMessage());
        assertEquals(attempt, envelope.getAttempt());
        assertTrue(envelope.getTimestamp() > 0);

        // And When unwrapping
        byte[] unwrappedData = processor.unwrap(envelope);

        // Then
        assertArrayEquals(originalData, unwrappedData);
    }

    /**
     * Verifies that the processor provides a fallback error message when the exception's 
     * detail message is null.
     */
    @Test
    void shouldHandleNullErrorMessage() {
        // Given
        byte[] data = {1, 2, 3};
        Exception error = new NullPointerException(); // throwable.getMessage() is often null here

        // When
        RetryEnvelope envelope = processor.wrap(data, 1, error, 0);

        // Then
        assertEquals("java.lang.NullPointerException", envelope.getErrorMessage());
    }
}
