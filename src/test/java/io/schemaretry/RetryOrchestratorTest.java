package io.schemaretry;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RetryOrchestrator}.
 * Focuses on verifying the registration and orchestration of annotated listeners.
 */
class RetryOrchestratorTest {

    /**
     * Verifies that the orchestrator correctly identifies and registers methods 
     * annotated with {@link RetryListener}.
     */
    @Test
    void shouldRegisterAnnotatedListeners() {
        // Given
        SchemaRetryConfig config = new SchemaRetryConfig();
        RedisStateStore stateStore = mock(RedisStateStore.class);
        org.apache.kafka.clients.producer.KafkaProducer producer = mock(org.apache.kafka.clients.producer.KafkaProducer.class);

        // We need to spy or mock the orchestrator to avoid starting real threads
        RetryOrchestrator orchestrator = Mockito.spy(new RetryOrchestrator(config, stateStore, producer) {
            @Override
            public <V> RetryConsumer<byte[], V> subscribe(String topic, String groupId, java.util.function.BiConsumer<V, RetryContext> handler, int maxAttempts) {
                // Return dummy to avoid starting real threads
                return null; 
            }
        });

        TestService service = new TestService();

        // When
        orchestrator.registerListeners(service);

        // Then
        // TestService has 2 topics in the annotation
        verify(orchestrator, times(2)).subscribe(anyString(), eq("test-group"), any(), eq(5));
        verify(orchestrator).subscribe(eq("topic-a"), eq("test-group"), any(), eq(5));
        verify(orchestrator).subscribe(eq("topic-b"), eq("test-group"), any(), eq(5));
    }

    /**
     * Helper class containing methods with and without @RetryListener annotations for testing.
     */
    static class TestService {
        @RetryListener(topics = {"topic-a", "topic-b"}, groupId = "test-group", maxAttempts = 5)
        public void handle(String payload, RetryContext context) {
            // logic
        }

        public void notAListener(String data) {}
    }
}
