package io.schemaretry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a method as a Kafka retry listener.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RetryListener {
    /**
     * The source topics to consume from.
     */
    String[] topics();

    /**
     * The Kafka consumer group ID.
     */
    String groupId();

    /**
     * Maximum number of retry attempts before moving to DLQ.
     * If not set, global configuration is used.
     */
    int maxAttempts() default -1;
}
