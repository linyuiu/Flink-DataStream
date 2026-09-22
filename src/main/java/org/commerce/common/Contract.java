package org.commerce.common;

import java.time.ZoneId;
import java.util.List;

public final class Contract {
    private Contract() {}

    public static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");
    public static final List<String> TYPES =
            List.of("ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "REFUND_SUCCEEDED");
    public static final List<String> CDC_TABLES =
            List.of(
                    "trade_outbox",
                    "dim_product",
                    "dim_sku",
                    "dim_category",
                    "dim_brand",
                    "dim_shop",
                    "dim_region");
    // 保留原有数组顺序以兼容状态和 Doris 列；Metric 枚举提供可读的业务访问方式。
    public static final String[] METRICS = {
        "created_orders",
        "cancelled_orders",
        "paid_orders",
        "paid_users",
        "refund_orders",
        "refund_requests",
        "paid_quantity",
        "created_goods_cent",
        "paid_goods_cent",
        "refund_goods_cent",
        "net_paid_goods_cent"
    };
}
