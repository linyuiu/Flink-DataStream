package org.linyureal.realtime.common;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.*;
import java.time.format.*;

public final class Jsons {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
            .withResolverStyle(ResolverStyle.STRICT);
    private Jsons() {}
    public static ObjectNode object(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            if (!n.isObject()) throw new IllegalArgumentException("Expected JSON object");
            return (ObjectNode) n;
        } catch (java.io.IOException e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }
    public static String write(Object value) {
        try { return MAPPER.writeValueAsString(value); }
        catch (java.io.IOException e) { throw new IllegalArgumentException("Cannot serialize JSON", e); }
    }
    public static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || !v.isTextual() || v.asText().isBlank())
            throw new IllegalArgumentException("Missing text: " + field);
        return v.asText();
    }
    public static long integer(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || !v.isIntegralNumber() || !v.canConvertToLong())
            throw new IllegalArgumentException("Missing integer: " + field);
        return v.longValue();
    }
    public static LocalDateTime time(JsonNode n, String field) {
        JsonNode v = n.get(field);
        // Configured CDC connect mode: DATETIME(3) is milliseconds of a timezone-free wall clock.
        if (v != null && v.isIntegralNumber())
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(v.longValue()), ZoneOffset.UTC).withNano(0);
        return LocalDateTime.parse(text(n, field), TIME);
    }
    public static ObjectNode dirty(String stage, String raw, String reason) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("stage", stage); n.put("raw", raw); n.put("reason", reason);
        n.put("observed_at", Instant.now().toString());
        return n;
    }
}
