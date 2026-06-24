package com.afif.cdc.smt;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

public class Sha256HashValue<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String INPUT_FIELD_CONFIG = "input.field";
    public static final String OUTPUT_FIELD_CONFIG = "output.field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    INPUT_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    "canonical_payload",
                    ConfigDef.Importance.HIGH,
                    "Input field containing the canonical payload string.")
            .define(
                    OUTPUT_FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    "hash",
                    ConfigDef.Importance.HIGH,
                    "Output field that stores the SHA-256 hex digest.");

    private String inputField;
    private String outputField;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, ?> parsed = CONFIG_DEF.parse(configs);
        inputField = (String) parsed.get(INPUT_FIELD_CONFIG);
        outputField = (String) parsed.get(OUTPUT_FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        Object canonicalPayload = ConnectRecordUtil.readField(record.value(), inputField);
        if (canonicalPayload == null) {
            throw new DataException("Cannot hash record because input.field=" + inputField + " is null or missing");
        }

        String hash = sha256Hex(String.valueOf(canonicalPayload));
        return ConnectRecordUtil.putStringField(record, outputField, hash);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new ConnectException("SHA-256 algorithm is not available", e);
        }
    }
}
