package org.linyureal.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.common.Json;
import org.linyureal.model.cdc.Change;
import org.linyureal.validation.TradeValidator;

/** Accepts mock envelope or Debezium JSON, with/without the schema wrapper.
 * Debezium numeric DATETIME fields require time.precision.mode=connect (see cdc config). */
public final class ChangeParser {
    private ChangeParser() {}
    public static Change parse(String value) throws Exception {
        JsonNode root = Json.MAPPER.readTree(value);
        if (root == null || root.isNull()) return null; // Kafka tombstone
        JsonNode p = root.has("payload") ? root.get("payload") : root;
        if(p == null || p.isNull()) return null;
        String table = p.has("table") ? Json.text(p,"table") : Json.text(p.path("source"),"table");
        String op = Json.text(p,"op");
        JsonNode row = "d".equals(op) ? p.get("before") : p.get("after");
        if(row == null || !row.isObject()) throw new IllegalArgumentException("Missing full row");
        ObjectNode normalized=((ObjectNode)row).deepCopy();
        // MySQL DATETIME with Debezium time.precision.mode=connect is a logical
        // millisecond timestamp without timezone. Decode in UTC to retain wall-clock fields.
        for(String field:new String[]{"update_time","pay_time","refund_time"}) {
            JsonNode time=normalized.get(field);
            if(time!=null && time.isIntegralNumber()) {
                normalized.put(field,java.time.LocalDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(time.longValue()),java.time.ZoneOffset.UTC)
                        .format(TradeValidator.TIME));
            }
        }
        Change c = new Change(table,op,normalized);
        TradeValidator.validate(c);
        return c;
    }
}
