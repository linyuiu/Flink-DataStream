package org.commerce.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {}

    public static ObjectNode object(String value) {
        try {
            JsonNode node = MAPPER.readTree(value);
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException("Expected JSON object");
            }
            return (ObjectNode) node;
        } catch (java.io.IOException exception) {
            throw new IllegalArgumentException("Malformed JSON", exception);
        }
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (java.io.IOException exception) {
            throw new IllegalArgumentException("Serialization failed", exception);
        }
    }

    public static String text(JsonNode row, String key) {
        JsonNode field = row.get(key);
        if (field == null || !field.isTextual() || field.asText().isBlank()) {
            throw new IllegalArgumentException("Invalid text: " + key);
        }
        return field.asText();
    }

    public static long number(JsonNode row, String key) {
        JsonNode field = row.get(key);
        if (field == null || !field.isIntegralNumber() || !field.canConvertToLong()) {
            throw new IllegalArgumentException("Invalid integer: " + key);
        }
        return field.longValue();
    }
}
