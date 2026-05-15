package io.schemaretry;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * The main entry point for the schema-retry library.
 * Coordinates the initialization of collaborators and manages the lifecycle of consumers.
 */
public class RetryOrchestrator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RetryOrchestrator.class);

    private final SchemaRetryConfig config;
    private final RedisStateStore stateStore;
    private final EnvelopeProcessor envelopeProcessor;
    private final BackoffStrategy backoffStrategy;
    private final RetryRouter retryRouter;
    private final KafkaProducer<byte[], Object> producer;
    private final AvroSerdeService avroSerdeService;

    private final List<RetryConsumer<?, ?>> consumers = new ArrayList<>();

    /**
     * Constructs a RetryOrchestrator with the given configuration.
     * Initializes Redis connectivity and Kafka producers.
     *
     * @param config The library configuration.
     */
    public RetryOrchestrator(SchemaRetryConfig config) {
        this(
            config,
            new RedisStateStore(
                config.getRedis().getUri(),
                Duration.ofHours(config.getRedis().getCountTtlHours()),
                Duration.ofHours(config.getRedis().getIdempotencyTtlHours())
            ),
            new KafkaProducer<>(config.getKafka().toProperties("schema-retry-router"))
        );
    }

    /**
     * Internal constructor for testing or manual component management.
     */
    protected RetryOrchestrator(SchemaRetryConfig config, RedisStateStore stateStore, KafkaProducer<byte[], Object> producer) {
        this.config = config;
        this.stateStore = stateStore;
        this.producer = producer;
        this.envelopeProcessor = new EnvelopeProcessor();
        this.backoffStrategy = new ExponentialBackoffStrategy(
                Duration.ofMillis(config.getRetry().getBackoff().getInitialIntervalMs()),
                config.getRetry().getBackoff().getMultiplier(),
                Duration.ofMillis(config.getRetry().getBackoff().getMaxIntervalMs())
        );
        this.retryRouter = new RetryRouter(stateStore, envelopeProcessor, backoffStrategy, producer);
        
        if (config.getSchemaRegistry() != null && config.getSchemaRegistry().getUrl() != null) {
            this.avroSerdeService = new AvroSerdeService(config.getSchemaRegistry().createClient());
            log.info("Avro deserialization enabled using Schema Registry at {}", config.getSchemaRegistry().getUrl());
        } else {
            this.avroSerdeService = null;
        }

        log.info("RetryOrchestrator initialized");
    }

    /**
     * Registers and starts a new consumer for a specific topic.
     *
     * @param topic      The Kafka topic to consume from.
     * @param groupId    The consumer group ID.
     * @param handler    The user-provided logic for processing messages.
     * @param <V>        The type of the message payload.
     * @return The created RetryConsumer instance.
     */
    public <V> RetryConsumer<byte[], V> subscribe(String topic, String groupId, BiConsumer<V, RetryContext> handler) {
        return subscribe(topic, groupId, handler, config.getRetry().getMaxAttempts());
    }

    /**
     * Registers and starts a new consumer with a custom max attempts setting.
     *
     * @param topic       The Kafka topic to consume from.
     * @param groupId     The consumer group ID.
     * @param handler     The user-provided logic for processing messages.
     * @param maxAttempts Maximum number of retry attempts for this consumer.
     * @param <V>         The type of the message payload.
     * @return The created RetryConsumer instance.
     */
    public <V> RetryConsumer<byte[], V> subscribe(String topic, String groupId, BiConsumer<V, RetryContext> handler, int maxAttempts) {
        RetryConsumer<byte[], V> consumer = new RetryConsumer<>(
                config.getKafka().toProperties(groupId),
                retryRouter,
                envelopeProcessor,
                avroSerdeService,
                handler,
                maxAttempts
        );
        
        consumer.start(topic);
        consumers.add(consumer);
        log.info("Subscribed to topic: {} with group: {}", topic, groupId);
        return consumer;
    }

    /**
     * Scans the provided bean for methods annotated with {@link RetryListener} 
     * and automatically registers them as consumers.
     *
     * @param bean The object instance containing listener methods.
     */
    public void registerListeners(Object bean) {
        Class<?> clazz = bean.getClass();
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(RetryListener.class)) {
                RetryListener listener = method.getAnnotation(RetryListener.class);
                
                BiConsumer<Object, RetryContext> handler = (payload, context) -> {
                    try {
                        method.invoke(bean, payload, context);
                    } catch (Exception e) {
                        log.error("Failed to invoke @RetryListener method {}", method.getName(), e);
                        context.retry(e);
                    }
                };

                int maxAttempts = listener.maxAttempts() != -1 ? listener.maxAttempts() : config.getRetry().getMaxAttempts();
                
                for (String topic : listener.topics()) {
                    subscribe(topic, listener.groupId(), handler, maxAttempts);
                }
            }
        }
    }

    /**
     * Shuts down all active consumers and closes underlying connections (Redis, Kafka).
     */
    @Override
    public void close() {
        log.info("Shutting down RetryOrchestrator...");
        for (RetryConsumer<?, ?> consumer : consumers) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
        }
        
        if (producer != null) {
            producer.close();
        }
        
        if (stateStore != null) {
            stateStore.close();
        }
    }

    /**
     * Returns the configuration used by this orchestrator.
     * @return The SchemaRetryConfig instance.
     */
    public SchemaRetryConfig getConfig() {
        return config;
    }
}
