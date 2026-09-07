package org.linyureal.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    private Json() {}
    public static String write(Object value) {
        try { return MAPPER.writeValueAsString(value); }
        catch (IOException e) { throw new IllegalArgumentException("JSON serialization failed", e); }
    }
    public static String text(JsonNode row, String field) {
        JsonNode value = row.get(field);
        if (value == null || !value.isTextual() || value.asText().trim().isEmpty())
            throw new IllegalArgumentException("Required text: " + field);
        return value.asText();
    }
    public static long number(JsonNode row, String field) {
        JsonNode value = row.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong())
            throw new IllegalArgumentException("Required integer: " + field);
        return value.longValue();
    }
}
