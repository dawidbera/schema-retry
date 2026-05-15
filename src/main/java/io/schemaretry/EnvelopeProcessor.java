package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Processor responsible for wrapping original message data and error metadata into a RetryEnvelope.
 */
public class EnvelopeProcessor {

    /**
     * Wraps message data into a RetryEnvelope.
     *
     * @param originalPayload The raw bytes of the original message.
     * @param schemaId        The Schema Registry ID of the original message.
     * @param throwable       The exception that caused the retry.
     * @param attempt         The current retry attempt number.
     * @return A populated RetryEnvelope instance.
     */
    public RetryEnvelope wrap(byte[] originalPayload, int schemaId, Throwable throwable, int attempt) {
        String stackTrace = getStackTraceSnippet(throwable);
        return RetryEnvelope.newBuilder()
                .setOriginalPayload(ByteBuffer.wrap(originalPayload))
                .setSchemaId(schemaId)
                .setErrorMessage(throwable.getMessage() != null ? throwable.getMessage() : throwable.getClass().getName())
                .setStacktrace(stackTrace)
                .setAttempt(attempt)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();
    }

    private String getStackTraceSnippet(Throwable throwable) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        String fullTrace = sw.toString();
        // Return first 2000 characters or full trace if shorter
        return fullTrace.length() > 2000 ? fullTrace.substring(0, 2000) : fullTrace;
    }

    /**
     * Unwraps the original payload from a RetryEnvelope.
     *
     * @param envelope The RetryEnvelope received from a retry topic.
     * @return The raw bytes of the original message.
     */
    public byte[] unwrap(RetryEnvelope envelope) {
        ByteBuffer buffer = envelope.getOriginalPayload();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
