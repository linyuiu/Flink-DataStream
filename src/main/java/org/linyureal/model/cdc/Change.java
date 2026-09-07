package org.linyureal.model.cdc;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;

public class Change implements Serializable {
    public String table;
    public String op;
    public ObjectNode after;
    public Change() {}
    public Change(String table, String op, ObjectNode after) {
        this.table = table; this.op = op; this.after = after;
    }
    public String orderId() { return org.linyureal.common.Json.text(after, "order_id"); }
}
