package org.linyureal.realtime.model;

/** String-only Flink POJO; no Jackson internal classes in network/state serializers. */
public class RowChange {
    public String table, operation, orderId, rowJson;
    public RowChange() {}
    public RowChange(String table, String operation, String orderId, String json) {
        this.table = table; this.operation = operation; this.orderId = orderId; this.rowJson = json;
    }
}
