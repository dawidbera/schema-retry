# schema-retry Basic Example

This is a sample application demonstrating how to use the `schema-retry` library to implement non-blocking retries in a Kafka-based Java application.

## Prerequisites
- Java 21+
- Apache Kafka & Confluent Schema Registry
- Redis

## How to run
1. Install the core library to your local Maven repository:
   ```bash
   cd ../..
   mvn install -DskipTests
   ```
2. Build the example application:
   ```bash
   cd examples/basic-example
   mvn clean compile
   ```
3. Ensure Kafka, Schema Registry, and Redis are running (you can adjust connection details in `src/main/resources/application.yaml`).
4. Run the application:
   ```bash
   mvn exec:java -Dexec.mainClass="io.schemaretry.example.Main"
   ```

## What it does
- Subscribes to the `orders` topic using `@RetryListener`.
- Automatically deserializes incoming messages into the `Order` Avro record.
- Processes orders based on their amount:
  - Amount > 1000: Triggers a retry (simulated timeout).
  - Amount < 0: Discards the message (fatal error).
  - Other: Processes successfully.
- If an order fails, it will be automatically routed to `orders-retry-2s`, `orders-retry-3s`, etc., and eventually to `orders-dlq` if max attempts are reached.
