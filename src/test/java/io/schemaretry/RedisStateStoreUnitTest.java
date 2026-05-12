package io.schemaretry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RedisStateStore}.
 */
@ExtendWith(MockitoExtension.class)
class RedisStateStoreUnitTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private StatefulRedisConnection<String, String> connection;

    @Mock
    private RedisCommands<String, String> syncCommands;

    private RedisStateStore stateStore;

    /**
     * Initializes the test environment before each test case.
     */
    @BeforeEach
    void setUp() {
        when(connection.sync()).thenReturn(syncCommands);
        stateStore = new RedisStateStore(redisClient, connection, Duration.ofHours(24), Duration.ofHours(1));
    }

    /**
     * Verifies that the retry count is correctly incremented in Redis.
     */
    @Test
    void shouldIncrementCount() {
        String msgId = "msg-123";
        String key = "schema-retry:count:" + msgId;
        
        when(syncCommands.incr(key)).thenReturn(1L);
        
        long count = stateStore.incrementCount(msgId);
        
        assertEquals(1, count);
        verify(syncCommands).incr(key);
        verify(syncCommands).expire(eq(key), anyLong());
    }

    /**
     * Verifies that the retry count is correctly retrieved from Redis.
     */
    @Test
    void shouldGetCount() {
        String msgId = "msg-123";
        String key = "schema-retry:count:" + msgId;
        
        when(syncCommands.get(key)).thenReturn("5");
        
        assertEquals(5, stateStore.getCount(msgId));
        verify(syncCommands).get(key);
    }

    /**
     * Verifies that a retry count of 0 is returned when no record exists in Redis.
     */
    @Test
    void shouldReturnZeroWhenNoCount() {
        when(syncCommands.get(anyString())).thenReturn(null);
        assertEquals(0, stateStore.getCount("unknown"));
    }

    /**
     * Verifies that idempotency checks correctly mark hashes as processed in Redis.
     */
    @Test
    void shouldCheckAndMarkIdempotent() {
        String hash = "hash123";
        String key = "schema-retry:idempotency:" + hash;
        
        // Success case (not processed before)
        when(syncCommands.set(eq(key), eq("1"), any(SetArgs.class))).thenReturn("OK");
        assertFalse(stateStore.checkAndMarkIdempotent(hash));
        
        // Failure case (already processed)
        when(syncCommands.set(eq(key), eq("1"), any(SetArgs.class))).thenReturn(null);
        assertTrue(stateStore.checkAndMarkIdempotent(hash));
    }
}
