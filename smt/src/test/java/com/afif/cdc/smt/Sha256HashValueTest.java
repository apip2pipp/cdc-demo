package com.afif.cdc.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class Sha256HashValueTest {
    @Test
    void addsSha256HashFromCanonicalPayload() {
        SourceRecord record = new SourceRecord(
                Map.of(),
                Map.of(),
                "cdc_postgres.public.orders",
                null,
                Map.of("canonical_payload", "{\"id\":1,\"name\":\"Afif\"}"));

        Sha256HashValue<SourceRecord> transform = new Sha256HashValue<>();
        transform.configure(Map.of());

        SourceRecord transformed = transform.apply(record);
        Map<?, ?> value = (Map<?, ?>) transformed.value();

        assertEquals(
                "afbd9d61333a80e533a6b3fb220af2ae531fd4d1399a07ac50a7d7b8863237b6",
                value.get("hash"));
    }
}
