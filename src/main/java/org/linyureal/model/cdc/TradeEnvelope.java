package org.linyureal.model.cdc;

import org.linyureal.common.Json;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Flink wire POJO: only Strings, so Jackson tree internals never enter generic Kryo serialization. */
public class TradeEnvelope {
    public String table, op, rowJson, orderKey;
    public TradeEnvelope() {}
    public TradeEnvelope(Change change) {
        table=change.table; op=change.op; rowJson=Json.write(change.after); orderKey=change.orderId();
    }
    public String orderId() { return orderKey; }
    public Change decode() throws Exception { return new Change(table,op,(ObjectNode)Json.MAPPER.readTree(rowJson)); }
}
