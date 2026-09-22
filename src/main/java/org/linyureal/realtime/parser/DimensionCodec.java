package org.linyureal.realtime.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.*;
import java.util.*;
import static org.linyureal.realtime.validation.RowValidator.require;

public final class DimensionCodec {
    private DimensionCodec() {}
    public static String parse(String raw) {
        JsonNode p = Jsons.object(raw); if (p.has("payload")) p = p.get("payload");
        String table = Jsons.text(p.path("source"), "table");
        require(Contracts.DIM_TABLES.contains(table) && List.of("r", "c", "u").contains(Jsons.text(p, "op")), "Unsupported dimension operation");
        JsonNode row = p.path("after");
        String type = table.equals("dim_region") ? Jsons.text(row, "region_level") : table.substring(4).toUpperCase(Locale.ROOT);
        require(!table.equals("dim_region") || List.of("PROVINCE", "CITY", "DISTRICT").contains(type), "Invalid region level");
        long version = Jsons.integer(row, "version"); require(version > 0, "Invalid dimension version");
        ObjectNode out = Jsons.MAPPER.createObjectNode();
        out.put("dimension_type", type); out.put("dimension_id", Jsons.text(row, "id"));
        out.put("name", Jsons.text(row, "name")); out.put("parent_id", Jsons.text(row, "parent_id"));
        out.put("version", version); out.put("attributes_json", row.toString());
        return out.toString();
    }
    public static String key(String json) {
        ObjectNode n = Jsons.object(json); return Jsons.text(n, "dimension_type") + ":" + Jsons.text(n, "dimension_id");
    }
}
