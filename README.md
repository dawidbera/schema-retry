# schema-retry: Kafka Retry Library

High-performance, non-blocking retry library for Java applications using Apache Kafka, Confluent Schema Registry, and Redis.

## 🚀 Overview

`schema-retry` solves the "head-of-line blocking" problem in Kafka consumers while maintaining strict data contracts. It provides a robust, schema-aware retry mechanism that scales with Java 21 Virtual Threads and manages state distributedly via Redis.

## 🏗 Microservices Architecture & Request Flow

The library implements a multi-tier retry strategy where failed messages are routed through a series of time-bucketed retry topics before eventually landing in a Dead Letter Queue (DLQ).

### Architecture Diagram

```mermaid
graph TD
    subgraph "Producer Layer"
        P[Original Producer]
    end

    subgraph "Main Processing"
        MT[Kafka: main-topic]
        C[RetryConsumer]
        L[User Listener]
    end

    subgraph "State & Management"
        R[(Redis)]
        SR[Schema Registry]
    end

    subgraph "Retry Infrastructure"
        RT1[Kafka: main-topic-retry-1s]
        RT2[Kafka: main-topic-retry-5m]
        DLQ[Kafka: main-topic-dlq]
    end

    P -->|1. Message| MT
    MT -->|2. Poll| C
    C <-->|3. State/CB Check| R
    C <-->|4. Get Schema| SR
    C -->|5. Execute| L
    
    L -->|6a. Success| MT
    L -->|6b. Recoverable Error| RT1
    RT1 -->|7. Delay & Reprocess| C
    L -->|6c. Fatal Error| DLQ
```

### Request Flow Sequence

```mermaid
sequenceDiagram
    participant K as Kafka (Main Topic)
    participant C as RetryConsumer (Virtual Thread)
    participant R as Redis (State/CB)
    participant L as @RetryListener (User Code)
    participant RT as Kafka (Retry/DLQ Topics)

    K->>C: 1. Poll Message
    C->>R: 2. Check Circuit Breaker Status
    alt Circuit OPEN
        C->>C: 3. Wait/Backoff
    else Circuit CLOSED
        C->>R: 4. Increment Attempt Count
        C->>L: 5. Invoke Handler
        alt Success
            L-->>C: 6a. Complete
            C->>K: 7a. Commit Offset
        else Recoverable Error
            L-->>C: 6b. context.retry(e)
            C->>RT: 7b. Publish to Retry Topic (with Envelope)
            C->>K: 8b. Commit Offset
        else Fatal Error / Max Attempts
            L-->>C: 6c. context.discard(e)
            C->>RT: 7c. Publish to DLQ Topic
            C->>K: 8c. Commit Offset
        end
    end
```

## 🛠 Key Features

- **Non-blocking Retries:** Prevents slow or failing messages from blocking the entire partition.
- **Virtual Thread Support:** Uses Java 21 `Executors.newVirtualThreadPerTaskExecutor()` for high-concurrency processing.
- **Schema Preservation:** Uses `RetryEnvelope` to wrap original payloads while keeping their Schema Registry IDs intact.
- **Distributed State:** Redis tracks retry counts and manages Circuit Breaker states across multiple service instances.
- **Metadata Headers:** Enriches Kafka records with error class, message, and attempt count for better observability.

## 🚦 Tech Stack

- **Language:** Java 21 (Loom/Virtual Threads)
- **Messaging:** Apache Kafka / Confluent Cloud
- **Schema:** Confluent Schema Registry (Avro)
- **State Store:** Redis (Lettuce)
- **Testing:** JUnit 5, Mockito, Testcontainers
