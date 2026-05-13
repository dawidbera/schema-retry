package io.schemaretry;

import java.time.Duration;

/**
 * Strategy for calculating retry delays and determining target topics.
 */
public interface BackoffStrategy {
    /**
     * Calculates the delay for a given attempt.
     * @param attempt The attempt number (0-based, where 0 is the first retry attempt).
     * @return The delay duration.
     */
    Duration calculateDelay(int attempt);

    /**
     * Determines the target topic for a given attempt.
     * @param originalTopic The original topic name.
     * @param attempt The attempt number.
     * @return The name of the retry topic.
     */
    String getTargetTopic(String originalTopic, int attempt);
}
