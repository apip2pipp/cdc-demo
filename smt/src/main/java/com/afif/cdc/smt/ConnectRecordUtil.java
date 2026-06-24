package com.afif.cdc.smt;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

final class ConnectRecordUtil {
    private ConnectRecordUtil() {
    }

    static Object readField(Object value, String fieldName) {
        if (value == null || fieldName == null || fieldName.isBlank()) {
            return null;
        }

        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            if (struct.schema().field(fieldName) == null) {
                return null;
            }
            return struct.get(fieldName);
        }

        if (value instanceof Map<?, ?>) {
            return ((Map<?, ?>) value).get(fieldName);
        }

        throw new DataException("Record value must be a Struct or Map, but was " + value.getClass().getName());
    }

    static <R extends ConnectRecord<R>> R putStringField(R record, String fieldName, String fieldValue) {
        Object value = record.value();
        if (value == null) {
            return record;
        }

        if (value instanceof Struct) {
            Struct original = (Struct) value;
            Schema updatedSchema = schemaWithStringField(original.schema(), fieldName);
            Struct updated = new Struct(updatedSchema);

            for (org.apache.kafka.connect.data.Field field : original.schema().fields()) {
                updated.put(field.name(), original.get(field));
            }
            updated.put(fieldName, fieldValue);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updated,
                    record.timestamp());
        }

        if (value instanceof Map<?, ?>) {
            Map<String, Object> updated = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                updated.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            updated.put(fieldName, fieldValue);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    updated,
                    record.timestamp());
        }

        throw new DataException("Record value must be a Struct or Map, but was " + value.getClass().getName());
    }

    private static Schema schemaWithStringField(Schema originalSchema, String fieldName) {
        SchemaBuilder builder = SchemaBuilder.struct();

        if (originalSchema.name() != null) {
            builder.name(originalSchema.name());
        }
        if (originalSchema.version() != null) {
            builder.version(originalSchema.version());
        }
        if (originalSchema.doc() != null) {
            builder.doc(originalSchema.doc());
        }
        if (originalSchema.isOptional()) {
            builder.optional();
        }
        if (originalSchema.parameters() != null) {
            originalSchema.parameters().forEach(builder::parameter);
        }

        for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
            builder.field(field.name(), field.schema());
        }

        if (originalSchema.field(fieldName) == null) {
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }

        return builder.build();
    }
}
