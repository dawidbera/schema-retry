package io.schemaretry;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of {@link RetryContext}.
 * Bridges the listener's retry/discard signals to the {@link RetryRouter}.
 */
public class DefaultRetryContext implements RetryContext {
    private final String topic;
    private final String messageId;
    private final byte[] payload;
    private final int schemaId;
    private final int attempt;
    private final int maxAttempts;
    private final RetryRouter retryRouter;
    private final AtomicBoolean handled = new AtomicBoolean(false);

    private Throwable resultCause;
    private Action resultAction = Action.NONE;

    public enum Action {
        NONE, RETRY, DISCARD
    }

    /**
     * Constructs a new DefaultRetryContext.
     *
     * @param topic       The original topic name.
     * @param messageId   Unique identifier for the message.
     * @param payload     The raw message payload.
     * @param schemaId    The Schema Registry ID of the message.
     * @param attempt     The current retry attempt number.
     * @param maxAttempts Maximum allowed retry attempts.
     * @param retryRouter Router for handling retry/discard actions.
     */
    public DefaultRetryContext(String topic, String messageId, byte[] payload, int schemaId, 
                               int attempt, int maxAttempts, RetryRouter retryRouter) {
        this.topic = topic;
        this.messageId = messageId;
        this.payload = payload;
        this.schemaId = schemaId;
        this.attempt = attempt;
        this.maxAttempts = maxAttempts;
        this.retryRouter = retryRouter;
    }

    @Override
    public void retry(Throwable throwable) {
        if (handled.compareAndSet(false, true)) {
            this.resultCause = throwable;
            this.resultAction = Action.RETRY;
            retryRouter.route(topic, messageId, payload, schemaId, throwable, maxAttempts);
        }
    }

    @Override
    public void discard(Throwable throwable) {
        if (handled.compareAndSet(false, true)) {
            this.resultCause = throwable;
            this.resultAction = Action.DISCARD;
            // Routing to max + 1 ensures it goes to DLQ in current RetryRouter implementation
            retryRouter.route(topic, messageId, payload, schemaId, throwable, -1); 
        }
    }

    @Override
    public int getAttempt() {
        return attempt;
    }

    /**
     * Returns the action taken on this message (RETRY, DISCARD, or NONE).
     *
     * @return The resulting Action.
     */
    public Action getResultAction() {
        return resultAction;
    }

    /**
     * Returns the cause (exception) that triggered the retry or discard action.
     *
     * @return The Throwable cause, or null if no action was taken.
     */
    public Throwable getResultCause() {
        return resultCause;
    }
}
