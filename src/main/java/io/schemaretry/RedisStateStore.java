package io.schemaretry;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;

/**
 * Handles state management using Redis for retry counts and idempotency checks.
 */
public class RedisStateStore implements AutoCloseable {
    private static final String COUNT_PREFIX = "schema-retry:count:";
    private static final String IDEMPOTENCY_PREFIX = "schema-retry:idempotency:";
    
    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> syncCommands;
    
    private final Duration countTtl;
    private final Duration idempotencyTtl;

    /**
     * Constructs a new RedisStateStore with a provided connection (useful for testing).
     */
    protected RedisStateStore(RedisClient redisClient, StatefulRedisConnection<String, String> connection, 
                             Duration countTtl, Duration idempotencyTtl) {
        this.redisClient = redisClient;
        this.connection = connection;
        this.syncCommands = connection.sync();
        this.countTtl = countTtl;
        this.idempotencyTtl = idempotencyTtl;
    }

    /**
     * Constructs a new RedisStateStore.
     * @param redisUri The Redis URI (e.g., "redis://localhost:6379").
     * @param countTtl TTL for retry count keys.
     * @param idempotencyTtl TTL for idempotency keys.
     */
    public RedisStateStore(String redisUri, Duration countTtl, Duration idempotencyTtl) {
        this.redisClient = RedisClient.create(redisUri);
        this.connection = redisClient.connect();
        this.syncCommands = connection.sync();
        this.countTtl = countTtl;
        this.idempotencyTtl = idempotencyTtl;
    }

    /**
     * Increments the retry count for a given message ID.
     * @param messageId The unique identifier of the message.
     * @return The new retry count.
     */
    public long incrementCount(String messageId) {
        String key = COUNT_PREFIX + messageId;
        long count = syncCommands.incr(key);
        syncCommands.expire(key, countTtl.toSeconds());
        return count;
    }

    /**
     * Gets the current retry count for a given message ID.
     * @param messageId The unique identifier of the message.
     * @return The retry count, or 0 if not found.
     */
    public long getCount(String messageId) {
        String key = COUNT_PREFIX + messageId;
        String val = syncCommands.get(key);
        return val != null ? Long.parseLong(val) : 0L;
    }

    /**
     * Checks if a message has already been processed (idempotency) and marks it as processed.
     * @param hash The unique hash of the message content or ID.
     * @return true if the message was ALREADY processed, false otherwise.
     */
    public boolean checkAndMarkIdempotent(String hash) {
        String key = IDEMPOTENCY_PREFIX + hash;
        // setIfAbsent (NX) returns "OK" if set, null if already exists
        String result = syncCommands.set(key, "1", io.lettuce.core.SetArgs.Builder.nx().ex(idempotencyTtl));
        return result == null;
    }

    /**
     * Closes the Redis connection and shuts down the Redis client.
     */
    @Override
    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
