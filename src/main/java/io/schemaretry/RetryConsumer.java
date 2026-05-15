package io.schemaretry;

import io.schemaretry.avro.RetryEnvelope;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Main orchestrator for consuming and retrying messages.
 * Uses Virtual Threads for non-blocking processing.
 */
public class RetryConsumer<K, V> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RetryConsumer.class);

    private final Consumer<byte[], Object> kafkaConsumer;
    private final RetryRouter retryRouter;
    private final EnvelopeProcessor envelopeProcessor;
    private final BiConsumer<V, RetryContext> handler;
    private final int maxAttempts;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService executor;

    /**
     * Constructs a new RetryConsumer with the specified properties and collaborators.
     *
     * @param consumerProps     Kafka consumer configuration properties.
     * @param retryRouter       Router for handling failed messages.
     * @param envelopeProcessor Processor for wrapping/unwrapping retry envelopes.
     * @param handler           User-provided logic for processing consumed messages.
     * @param maxAttempts       Maximum number of retry attempts.
     */
    public RetryConsumer(Properties consumerProps, 
                         RetryRouter retryRouter, 
                         EnvelopeProcessor envelopeProcessor,
                         BiConsumer<V, RetryContext> handler,
                         int maxAttempts) {
        this(new KafkaConsumer<>(consumerProps), retryRouter, envelopeProcessor, handler, maxAttempts);
    }

    /**
     * Internal constructor for RetryConsumer, allowing for a pre-configured KafkaConsumer (useful for testing).
     *
     * @param kafkaConsumer     The Kafka consumer instance to use.
     * @param retryRouter       Router for handling failed messages.
     * @param envelopeProcessor Processor for wrapping/unwrapping retry envelopes.
     * @param handler           User-provided logic for processing consumed messages.
     * @param maxAttempts       Maximum number of retry attempts.
     */
    protected RetryConsumer(Consumer<byte[], Object> kafkaConsumer,
                          RetryRouter retryRouter,
                          EnvelopeProcessor envelopeProcessor,
                          BiConsumer<V, RetryContext> handler,
                          int maxAttempts) {
        this.kafkaConsumer = kafkaConsumer;
        this.retryRouter = retryRouter;
        this.envelopeProcessor = envelopeProcessor;
        this.handler = handler;
        this.maxAttempts = maxAttempts;
        // Java 21 Virtual Thread Per Task Executor
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Starts the consumer loop in a virtual thread.
     * Subscribes to the specified topic and begins polling for records.
     *
     * @param topic The Kafka topic to subscribe to.
     */
    public void start(String topic) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        
        Thread.ofVirtual().name("retry-consumer-loop").start(() -> {
            try {
                while (running.get()) {
                    ConsumerRecords<byte[], Object> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<byte[], Object> record : records) {
                        executor.submit(() -> processRecord(record));
                    }
                }
            } catch (Exception e) {
                log.error("Error in consumer loop", e);
            } finally {
                kafkaConsumer.close();
            }
        });
    }

    /**
     * Processes a single Kafka record.
     * Handles unwrapping of retry envelopes, circuit breaker checks, and execution of the user-provided handler.
     *
     * @param record The Kafka consumer record to process.
     */
    @SuppressWarnings("unchecked")
    void processRecord(ConsumerRecord<byte[], Object> record) {
        String messageId = record.key() != null ? new String(record.key()) : "unknown-" + System.nanoTime();
        
        // Circuit Breaker check
        String cbStatus = retryRouter.getStateStore().getCircuitBreakerStatus(record.topic());
        if ("OPEN".equals(cbStatus)) {
            log.warn("Circuit is OPEN for topic {}, delaying processing of message {}", record.topic(), messageId);
            // In a real scenario, we might want to pause the consumer or use a specific delay.
            // For MVP, we'll route it back to the same topic or a retry topic.
            return;
        }

        // Idempotency check
        if (retryRouter.getStateStore().checkAndMarkIdempotent(messageId)) {
            log.info("Message {} already processed (idempotent), skipping", messageId);
            return;
        }

        byte[] payload;
        int schemaId = -1; // In a real scenario, extract from Kafka headers or magic bytes
        int currentAttempt = 0;

        try {
            Object value = record.value();
            if (value instanceof RetryEnvelope envelope) {
                payload = envelopeProcessor.unwrap(envelope);
                schemaId = envelope.getSchemaId();
                currentAttempt = envelope.getAttempt();
            } else if (value instanceof byte[] bytes) {
                payload = bytes;
            } else {
                log.error("Unsupported record value type: {}", value.getClass().getName());
                return;
            }

            DefaultRetryContext context = new DefaultRetryContext(
                    record.topic(), 
                    messageId, 
                    payload, 
                    schemaId, 
                    currentAttempt, 
                    maxAttempts, 
                    retryRouter
            );

            try {
                // Here we'd normally deserialize the payload to V if it's still bytes
                // For MVP, assuming handler can take the payload
                handler.accept((V) payload, context);
                
                // If handler didn't call retry/discard, we consider it a success
                if (context.getResultAction() == DefaultRetryContext.Action.NONE) {
                    log.debug("Message {} processed successfully", messageId);
                }
            } catch (Exception e) {
                log.error("Handler failed for message {}", messageId, e);
                context.retry(e);
            }

        } catch (Exception e) {
            log.error("Failed to process record from topic {}", record.topic(), e);
        }
    }

    /**
     * Stops the consumer and shuts down the executor service.
     */
    @Override
    public void close() {
        running.set(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
