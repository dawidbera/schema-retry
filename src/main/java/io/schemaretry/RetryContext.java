package io.schemaretry;

/**
 * Context provided to the listener to control the retry flow.
 */
public interface RetryContext {
    /**
     * Signals that the message should be retried due to a recoverable error.
     * @param throwable The cause of the retry.
     */
    void retry(Throwable throwable);

    /**
     * Signals that the message should be discarded (sent to DLQ) due to a fatal error.
     * @param throwable The cause of the failure.
     */
    void discard(Throwable throwable);

    /**
     * Returns the current retry attempt number.
     * @return The attempt count (0 for the first attempt).
     */
    int getAttempt();
}
