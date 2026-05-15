package io.schemaretry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvroSerdeServiceTest {

    @Mock
    private SchemaRegistryClient schemaRegistryClient;

    private AvroSerdeService avroSerdeService;

    private Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        avroSerdeService = new AvroSerdeService(schemaRegistryClient);
        schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"field\",\"type\":\"string\"}]}");
    }

    @Test
    void shouldExtractSchemaId() {
        // Given
        byte[] payload = ByteBuffer.allocate(5).put((byte) 0).putInt(123).array();

        // When
        int schemaId = avroSerdeService.extractSchemaId(payload);

        // Then
        assertEquals(123, schemaId);
    }

    @Test
    void shouldReturnMinusOneForInvalidPayload() {
        assertAll(
            () -> assertEquals(-1, avroSerdeService.extractSchemaId(null)),
            () -> assertEquals(-1, avroSerdeService.extractSchemaId(new byte[]{1, 2, 3})),
            () -> assertEquals(-1, avroSerdeService.extractSchemaId(new byte[]{1, 0, 0, 0, 1}))
        );
    }
}
