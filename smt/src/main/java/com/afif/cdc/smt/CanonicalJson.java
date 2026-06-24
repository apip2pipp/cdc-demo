package com.afif.cdc.smt;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

final class CanonicalJson {
    private CanonicalJson() {
    }

    static String stringify(Object value) {
        StringBuilder json = new StringBuilder();
        writeValue(json, value);
        return json.toString();
    }

    private static void writeValue(StringBuilder json, Object value) {
        if (value == null) {
            json.append("null");
        } else if (value instanceof Struct) {
            writeStruct(json, (Struct) value);
        } else if (value instanceof Map<?, ?>) {
            writeMap(json, (Map<?, ?>) value);
        } else if (value instanceof Collection<?>) {
            writeCollection(json, (Collection<?>) value);
        } else if (value.getClass().isArray()) {
            writeArray(json, value);
        } else if (value instanceof CharSequence || value instanceof Character || value instanceof TemporalAccessor) {
            writeString(json, value.toString());
        } else if (value instanceof Boolean) {
            json.append(value);
        } else if (value instanceof BigDecimal) {
            json.append(((BigDecimal) value).stripTrailingZeros().toPlainString());
        } else if (value instanceof Number) {
            json.append(value);
        } else if (value instanceof byte[]) {
            writeString(json, Base64.getEncoder().encodeToString((byte[]) value));
        } else if (value instanceof ByteBuffer) {
            ByteBuffer duplicate = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            writeString(json, Base64.getEncoder().encodeToString(bytes));
        } else {
            writeString(json, value.toString());
        }
    }

    private static void writeStruct(StringBuilder json, Struct struct) {
        TreeMap<String, Object> sorted = new TreeMap<>();
        for (Field field : struct.schema().fields()) {
            sorted.put(field.name(), struct.get(field));
        }
        writeSortedEntries(json, sorted);
    }

    private static void writeMap(StringBuilder json, Map<?, ?> map) {
        TreeMap<String, Object> sorted = new TreeMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            sorted.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        writeSortedEntries(json, sorted);
    }

    private static void writeSortedEntries(StringBuilder json, TreeMap<String, Object> sorted) {
        json.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : sorted.entrySet()) {
            if (!first) {
                json.append(',');
            }
            writeString(json, entry.getKey());
            json.append(':');
            writeValue(json, entry.getValue());
            first = false;
        }
        json.append('}');
    }

    private static void writeCollection(StringBuilder json, Collection<?> collection) {
        json.append('[');
        boolean first = true;
        for (Object item : collection) {
            if (!first) {
                json.append(',');
            }
            writeValue(json, item);
            first = false;
        }
        json.append(']');
    }

    private static void writeArray(StringBuilder json, Object array) {
        List<Object> values = new ArrayList<>();
        if (array instanceof Object[]) {
            for (Object item : (Object[]) array) {
                values.add(item);
            }
        } else if (array instanceof int[]) {
            for (int item : (int[]) array) {
                values.add(item);
            }
        } else if (array instanceof long[]) {
            for (long item : (long[]) array) {
                values.add(item);
            }
        } else if (array instanceof double[]) {
            for (double item : (double[]) array) {
                values.add(item);
            }
        } else if (array instanceof boolean[]) {
            for (boolean item : (boolean[]) array) {
                values.add(item);
            }
        } else if (array instanceof byte[]) {
            writeString(json, Base64.getEncoder().encodeToString((byte[]) array));
            return;
        } else {
            writeString(json, array.toString());
            return;
        }
        writeCollection(json, values);
    }

    private static void writeString(StringBuilder json, String value) {
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    json.append("\\\"");
                    break;
                case '\\':
                    json.append("\\\\");
                    break;
                case '\b':
                    json.append("\\b");
                    break;
                case '\f':
                    json.append("\\f");
                    break;
                case '\n':
                    json.append("\\n");
                    break;
                case '\r':
                    json.append("\\r");
                    break;
                case '\t':
                    json.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
            }
        }
        json.append('"');
    }
}
