package org.commerce.common;

/** 指标名称与状态数组位置的固定映射。已有 index 不可调整，否则会读错历史 checkpoint。 */
public enum Metric {
    CREATED_ORDERS(0, "created_orders"),
    CANCELLED_ORDERS(1, "cancelled_orders"),
    PAID_ORDERS(2, "paid_orders"),
    PAID_USERS(3, "paid_users"),
    REFUND_ORDERS(4, "refund_orders"),
    REFUND_REQUESTS(5, "refund_requests"),
    PAID_QUANTITY(6, "paid_quantity"),
    CREATED_GOODS_CENT(7, "created_goods_cent"),
    PAID_GOODS_CENT(8, "paid_goods_cent"),
    REFUND_GOODS_CENT(9, "refund_goods_cent"),
    NET_PAID_GOODS_CENT(10, "net_paid_goods_cent");

    private final int index;
    private final String columnName;

    Metric(int index, String columnName) {
        this.index = index;
        this.columnName = columnName;
    }

    public int index() {
        return index;
    }

    public String columnName() {
        return columnName;
    }
}
