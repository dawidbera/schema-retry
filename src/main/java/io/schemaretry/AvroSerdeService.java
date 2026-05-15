package io.schemaretry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.nio.ByteBuffer;

/**
 * Service for handling Avro serialization and deserialization using Schema Registry.
 */
public class AvroSerdeService {
    private final KafkaAvroDeserializer deserializer;

    /**
     * Constructs a new AvroSerdeService with the specified SchemaRegistryClient.
     *
     * @param client The Schema Registry client to use for fetching schemas.
     */
    public AvroSerdeService(SchemaRegistryClient client) {
        this.deserializer = new KafkaAvroDeserializer(client);
    }

    /**
     * Deserializes a payload using Schema Registry.
     * Handles both Confluent wire format (with 5-byte header) and raw payloads with an explicit schema ID.
     *
     * @param topic    Kafka topic name.
     * @param payload  The byte array to deserialize.
     * @param schemaId The schema ID (if known), or -1 if embedded in payload.
     * @return The deserialized object.
     */
    public Object deserialize(String topic, byte[] payload, int schemaId) {
        if (payload == null) {
            return null;
        }

        // If schemaId is provided and payload doesn't have the Confluent header, reconstruct it
        if (schemaId != -1 && (payload.length < 5 || payload[0] != 0)) {
            ByteBuffer buffer = ByteBuffer.allocate(5 + payload.length);
            buffer.put((byte) 0);
            buffer.putInt(schemaId);
            buffer.put(payload);
            return deserializer.deserialize(topic, buffer.array());
        }

        return deserializer.deserialize(topic, payload);
    }

    /**
     * Extracts the Schema Registry ID from a Confluent-formatted byte array.
     *
     * @param payload The byte array containing the magic byte and schema ID.
     * @return The extracted schema ID, or -1 if the format is invalid.
     */
    public int extractSchemaId(byte[] payload) {
        if (payload != null && payload.length >= 5 && payload[0] == 0) {
            return ByteBuffer.wrap(payload).getInt(1);
        }
        return -1;
    }
}
