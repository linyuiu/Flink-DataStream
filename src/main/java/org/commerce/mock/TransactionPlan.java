package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.commerce.model.TradeEvent;

import java.util.*;

/** 一次业务动作对应一个数据库事务；业务行与 event 对应的 Outbox 行必须一起提交。 */
public class TransactionPlan {
    public final List<Row> rows = new ArrayList<>();
    public TradeEvent event;

    public TransactionPlan row(String table, boolean update, ObjectNode value) {
        rows.add(new Row(table, update, value));
        return this;
    }

    public TransactionPlan insert(String table, ObjectNode value) {
        return row(table, false, value);
    }

    public TransactionPlan update(String table, ObjectNode value) {
        return row(table, true, value);
    }

    public TransactionPlan event(TradeEvent value) {
        event = value;
        return this;
    }

    public static class Row {
        public final String table;
        public final boolean update;
        public final ObjectNode value;

        Row(String table, boolean update, ObjectNode value) {
            this.table = table;
            this.update = update;
            this.value = value;
        }
    }
}
