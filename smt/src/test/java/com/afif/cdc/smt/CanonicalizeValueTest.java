package com.afif.cdc.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class CanonicalizeValueTest {
    @Test
    void canonicalizesStructAfterFieldWithSortedKeys() {
        Schema rowSchema = SchemaBuilder.struct()
                .name("orders.Value")
                .optional()
                .field("name", Schema.STRING_SCHEMA)
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("orders.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct after = new Struct(rowSchema)
                .put("name", "Afif")
                .put("id", 1);
        Struct envelope = new Struct(envelopeSchema)
                .put("after", after)
                .put("op", "c");
        SourceRecord record = new SourceRecord(Map.of(), Map.of(), "cdc_postgres.public.orders", envelopeSchema, envelope);

        CanonicalizeValue<SourceRecord> transform = new CanonicalizeValue<>();
        transform.configure(Map.of());

        SourceRecord transformed = transform.apply(record);
        Struct value = (Struct) transformed.value();

        assertEquals("{\"id\":1,\"name\":\"Afif\"}", value.getString("canonical_payload"));
    }

    @Test
    void canonicalizesSchemalessMapWithSortedKeys() {
        SourceRecord record = new SourceRecord(
                Map.of(),
                Map.of(),
                "cdc_postgres.public.orders",
                null,
                Map.of("after", Map.of("name", "Afif", "id", 1), "op", "c"));

        CanonicalizeValue<SourceRecord> transform = new CanonicalizeValue<>();
        transform.configure(Map.of());

        SourceRecord transformed = transform.apply(record);
        Map<?, ?> value = (Map<?, ?>) transformed.value();

        assertEquals("{\"id\":1,\"name\":\"Afif\"}", value.get("canonical_payload"));
    }

    @Test
    void usesBeforeAsFallbackForDeleteEvents() {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("before", Map.of("name", "Afif", "id", 1));
        envelope.put("after", null);
        envelope.put("op", "d");

        SourceRecord record = new SourceRecord(
                Map.of(),
                Map.of(),
                "cdc_postgres.public.orders",
                null,
                envelope);

        CanonicalizeValue<SourceRecord> transform = new CanonicalizeValue<>();
        transform.configure(Map.of());

        SourceRecord transformed = transform.apply(record);
        Map<?, ?> value = (Map<?, ?>) transformed.value();

        assertEquals("{\"id\":1,\"name\":\"Afif\"}", value.get("canonical_payload"));
    }
}
