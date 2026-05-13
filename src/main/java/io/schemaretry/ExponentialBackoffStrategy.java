package io.schemaretry;

import java.time.Duration;

/**
 * Exponential backoff implementation of {@link BackoffStrategy}.
 */
public class ExponentialBackoffStrategy implements BackoffStrategy {
    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;

    /**
     * Constructs an ExponentialBackoffStrategy.
     *
     * @param initialDelay The delay for the first retry attempt (attempt 0).
     * @param multiplier   The factor by which the delay increases for each subsequent attempt.
     * @param maxDelay     The maximum allowable delay.
     */
    public ExponentialBackoffStrategy(Duration initialDelay, double multiplier, Duration maxDelay) {
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
    }

    /**
     * Calculates the exponential delay for the given attempt, capped at maxDelay.
     *
     * @param attempt The attempt number (0-based).
     * @return The calculated delay Duration.
     */
    @Override
    public Duration calculateDelay(int attempt) {
        // delay = initialDelay * (multiplier ^ attempt)
        long delayMs = (long) (initialDelay.toMillis() * Math.pow(multiplier, attempt));
        Duration delay = Duration.ofMillis(delayMs);
        
        if (delay.compareTo(maxDelay) > 0) {
            return maxDelay;
        }
        return delay;
    }

    /**
     * Generates a retry topic name based on the original topic and the calculated delay.
     * The topic name follows the pattern: {originalTopic}-retry-{formattedDelay} (e.g., orders-retry-30s).
     *
     * @param originalTopic The original topic name.
     * @param attempt       The attempt number.
     * @return The formatted retry topic name.
     */
    @Override
    public String getTargetTopic(String originalTopic, int attempt) {
        Duration delay = calculateDelay(attempt);
        return String.format("%s-retry-%s", originalTopic, formatDuration(delay));
    }

    /**
     * Formats a duration into a human-readable string for topic naming (e.g., 30s, 5m, 1h).
     *
     * @param duration The duration to format.
     * @return A string representation of the duration.
     */
    private String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m";
        }
        long hours = minutes / 60;
        return hours + "h";
    }
}
