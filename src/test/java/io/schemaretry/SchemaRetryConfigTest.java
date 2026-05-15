package io.schemaretry;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SchemaRetryConfig}.
 * Verifies that configuration can be loaded from YAML and that default values are correctly set.
 */
class SchemaRetryConfigTest {

    /**
     * Verifies that the configuration object can be correctly deserialized from a YAML input stream.
     *
     * @throws IOException if the YAML processing fails.
     */
    @Test
    void shouldLoadFromYaml() throws IOException {
        String yaml = """
                redis:
                  uri: redis://remote-host:6380
                  count-ttl-hours: 48
                retry:
                  max-attempts: 5
                  backoff:
                    initial-interval-ms: 2000
                    multiplier: 3.0
                """;

        SchemaRetryConfig config = SchemaRetryConfig.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals("redis://remote-host:6380", config.getRedis().getUri());
        assertEquals(48, config.getRedis().getCountTtlHours());
        assertEquals(5, config.getRetry().getMaxAttempts());
        assertEquals(2000, config.getRetry().getBackoff().getInitialIntervalMs());
        assertEquals(3.0, config.getRetry().getBackoff().getMultiplier());
    }

    /**
     * Verifies that the configuration object has sensible default values when initialized.
     */
    @Test
    void shouldHaveDefaultValues() {
        SchemaRetryConfig config = new SchemaRetryConfig();
        assertEquals("redis://localhost:6379", config.getRedis().getUri());
        assertEquals(3, config.getRetry().getMaxAttempts());
        assertEquals(1000, config.getRetry().getBackoff().getInitialIntervalMs());
    }
}
