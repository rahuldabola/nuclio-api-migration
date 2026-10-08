package dev.rahuldabola.quoteanalytics;

import dev.rahuldabola.quotes.avro.RegionStats;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the evolution rules Schema Registry enforces on quote-region-stats (BACKWARD):
 * consumers on the new schema must still read records written with the old one.
 */
class SchemaCompatibilityTest {

    private static final AvroSchema CURRENT = new AvroSchema(RegionStats.getClassSchema());

    @Test
    void addingAFieldWithADefaultIsBackwardCompatible() {
        AvroSchema next = withExtraField("{\"name\": \"currency\", \"type\": \"string\", \"default\": \"INR\"}");
        assertTrue(backwardErrors(next).isEmpty(), () -> backwardErrors(next).toString());
    }

    @Test
    void addingARequiredFieldIsRejected() {
        AvroSchema next = withExtraField("{\"name\": \"currency\", \"type\": \"string\"}");
        assertFalse(backwardErrors(next).isEmpty());
    }

    private static List<String> backwardErrors(AvroSchema next) {
        return next.isBackwardCompatible(CURRENT);
    }

    private static AvroSchema withExtraField(String fieldJson) {
        String schema = RegionStats.getClassSchema().toString();
        int fieldsEnd = schema.lastIndexOf(']');
        return new AvroSchema(schema.substring(0, fieldsEnd) + "," + fieldJson + schema.substring(fieldsEnd));
    }
}
