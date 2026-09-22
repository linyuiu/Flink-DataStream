package org.linyureal.realtime.common;

import java.util.List;

/** Table names are contracts, connection addresses and topic names are configuration. */
public final class Contracts {
    private Contracts() {}
    public static final List<String> TRADE_TABLES = List.of("order_header", "order_line", "payment_attempt",
            "payment_ledger", "payment_line", "refund_header", "refund_line");
    public static final List<String> DIM_TABLES = List.of("dim_product", "dim_sku", "dim_category",
            "dim_brand", "dim_shop", "dim_region");
    public static final String[] METRICS = {"created_order_count", "cancelled_order_count", "paid_order_count",
            "paid_user_count", "refund_order_count", "refund_request_count", "paid_quantity",
            "created_amount_cent", "paid_amount_cent", "refund_amount_cent", "net_paid_amount_cent"};
}
