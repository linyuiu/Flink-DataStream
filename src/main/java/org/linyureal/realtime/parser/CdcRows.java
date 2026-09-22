package org.linyureal.realtime.parser;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.RowChange;
import org.linyureal.realtime.validation.RowValidator;
import java.util.List;

public final class CdcRows {
    private CdcRows() {}
    public static RowChange trade(String raw) {
        ObjectNode envelope = Jsons.object(raw);
        JsonNode p = envelope.has("payload") ? envelope.get("payload") : envelope;
        String table = Jsons.text(p.path("source"), "table");
        String operation = Jsons.text(p, "op");
        JsonNode row = p.get("d".equals(operation) ? "before" : "after");
        if (row == null || !row.isObject()) throw new IllegalArgumentException("Full row image required");
        ObjectNode n = ((ObjectNode) row).deepCopy();
        for (String f : List.of("updated_at", "created_at", "cancelled_at", "paid_at", "succeeded_at"))
            if (n.hasNonNull(f)) n.put(f, Jsons.time(n, f).format(Jsons.TIME));
        RowChange c = new RowChange(table, operation, Jsons.text(n, "order_id"), n.toString());
        RowValidator.validate(c);
        return c;
    }
}
