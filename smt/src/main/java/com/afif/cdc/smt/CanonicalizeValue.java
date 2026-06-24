package com.afif.cdc.smt;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

public class CanonicalizeValue<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String INPUT_FIELD_CONFIG = "input.field";
    public static final String FALLBACK_FIELD_CONFIG = "fallback.field";
    public static final String OUTPUT_FIELD_CONFIG = "output.field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    INPUT_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    "after",
                    ConfigDef.Importance.HIGH,
                    "Debezium envelope field used as the canonicalization source.")
            .define(
                    FALLBACK_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    "before",
                    ConfigDef.Importance.MEDIUM,
                    "Fallback Debezium envelope field when input.field is null.")
            .define(
                    OUTPUT_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    "canonical_payload",
                    ConfigDef.Importance.HIGH,
                    "Output field that stores the canonical JSON string.");

    private String inputField;
    private String fallbackField;
    private String outputField;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, ?> parsed = CONFIG_DEF.parse(configs);
        inputField = (String) parsed.get(INPUT_FIELD_CONFIG);
        fallbackField = (String) parsed.get(FALLBACK_FIELD_CONFIG);
        outputField = (String) parsed.get(OUTPUT_FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        Object selected = ConnectRecordUtil.readField(record.value(), inputField);
        if (selected == null && fallbackField != null && !fallbackField.isBlank()) {
            selected = ConnectRecordUtil.readField(record.value(), fallbackField);
        }

        if (selected == null) {
            throw new DataException("Cannot canonicalize record because both input.field=" + inputField
                    + " and fallback.field=" + fallbackField + " are null or missing");
        }

        String canonicalPayload = CanonicalJson.stringify(selected);
        return ConnectRecordUtil.putStringField(record, outputField, canonicalPayload);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
