package io.schemaretry;

/**
 * Base exception for schema-retry library errors.
 */
public class RetryException extends RuntimeException {
    /**
     * Constructs a new RetryException with the specified detail message.
     * @param message The detail message.
     */
    public RetryException(String message) {
        super(message);
    }

    /**
     * Constructs a new RetryException with the specified detail message and cause.
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
    public RetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
