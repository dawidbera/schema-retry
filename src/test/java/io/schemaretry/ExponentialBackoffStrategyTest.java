package io.schemaretry;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link ExponentialBackoffStrategy}.
 */
class ExponentialBackoffStrategyTest {

    /**
     * Verifies that the delay increases exponentially based on the attempt number.
     */
    @Test
    void shouldCalculateExponentialDelay() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofSeconds(1), 2.0, Duration.ofMinutes(10));

        assertEquals(Duration.ofSeconds(1), strategy.calculateDelay(0));
        assertEquals(Duration.ofSeconds(2), strategy.calculateDelay(1));
        assertEquals(Duration.ofSeconds(4), strategy.calculateDelay(2));
        assertEquals(Duration.ofSeconds(8), strategy.calculateDelay(3));
    }

    /**
     * Verifies that the calculated delay does not exceed the specified maximum delay.
     */
    @Test
    void shouldCapAtMaxDelay() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofSeconds(1), 2.0, Duration.ofSeconds(5));

        assertEquals(Duration.ofSeconds(1), strategy.calculateDelay(0));
        assertEquals(Duration.ofSeconds(2), strategy.calculateDelay(1));
        assertEquals(Duration.ofSeconds(4), strategy.calculateDelay(2));
        assertEquals(Duration.ofSeconds(5), strategy.calculateDelay(3));
        assertEquals(Duration.ofSeconds(5), strategy.calculateDelay(10));
    }

    /**
     * Verifies that retry topic names are correctly formatted with seconds and minutes.
     */
    @Test
    void shouldFormatTargetTopicName() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofSeconds(30), 2.0, Duration.ofHours(1));

        assertEquals("orders-retry-30s", strategy.getTargetTopic("orders", 0));
        assertEquals("orders-retry-1m", strategy.getTargetTopic("orders", 1));
        assertEquals("orders-retry-2m", strategy.getTargetTopic("orders", 2));
    }

    /**
     * Verifies that retry topic names are correctly formatted with hours.
     */
    @Test
    void shouldFormatHoursInTopicName() {
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofHours(1), 1.0, Duration.ofHours(5));

        assertEquals("orders-retry-1h", strategy.getTargetTopic("orders", 0));
    }
}
