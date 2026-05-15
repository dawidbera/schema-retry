package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;

/**
 * Orchestrates the routing of failed messages to retry topics or DLQ.
 */
public class RetryRouter {
    private final RedisStateStore stateStore;
    private final EnvelopeProcessor envelopeProcessor;
    private final BackoffStrategy backoffStrategy;
    private final Producer<byte[], Object> producer;

    /**
     * Constructs a RetryRouter with the necessary collaborators.
     *
     * @param stateStore        Store for tracking retry counts.
     * @param envelopeProcessor Processor for wrapping/unwrapping retry envelopes.
     * @param backoffStrategy   Strategy for calculating delays and target topics.
     * @param producer          Kafka producer for sending messages to retry/DLQ topics.
     */
    public RetryRouter(RedisStateStore stateStore, 
                       EnvelopeProcessor envelopeProcessor, 
                       BackoffStrategy backoffStrategy, 
                       Producer<byte[], Object> producer) {
        this.stateStore = stateStore;
        this.envelopeProcessor = envelopeProcessor;
        this.backoffStrategy = backoffStrategy;
        this.producer = producer;
    }

    /**
     * Routes a failed message to the next appropriate topic.
     *
     * @param originalTopic The topic where the message was originally consumed.
     * @param messageId     Unique identifier for the message (used for state tracking).
     * @param payload       The raw bytes of the original message.
     * @param schemaId      The Schema Registry ID of the original message.
     * @param throwable     The cause of the failure.
     * @param maxAttempts   Maximum number of allowed retry attempts.
     */
    public void route(String originalTopic, String messageId, byte[] payload, int schemaId, Throwable throwable, int maxAttempts) {
        long attempt = stateStore.incrementCount(messageId);
        
        RetryEnvelope envelope = envelopeProcessor.wrap(payload, schemaId, throwable, (int) attempt);
        String targetTopic;
        
        if (attempt > maxAttempts) {
            targetTopic = originalTopic + "-dlq";
        } else {
            // attempt is 1-based here, backoff strategy usually expects 0-based for first retry
            targetTopic = backoffStrategy.getTargetTopic(originalTopic, (int) attempt - 1);
        }

        ProducerRecord<byte[], Object> record = new ProducerRecord<>(
                targetTopic, 
                messageId.getBytes(StandardCharsets.UTF_8), 
                envelope
        );

        // Add error metadata to headers for easier debugging in Kafka tools
        record.headers().add(new RecordHeader("x-retry-error-class", throwable.getClass().getName().getBytes(StandardCharsets.UTF_8)));
        if (throwable.getMessage() != null) {
            record.headers().add(new RecordHeader("x-retry-error-message", throwable.getMessage().getBytes(StandardCharsets.UTF_8)));
        }
        record.headers().add(new RecordHeader("x-retry-attempt", String.valueOf(attempt).getBytes(StandardCharsets.UTF_8)));

        producer.send(record);
    }

    /**
     * Returns the state store used by this router.
     *
     * @return The RedisStateStore instance.
     */
    public RedisStateStore getStateStore() {
        return stateStore;
    }
}
