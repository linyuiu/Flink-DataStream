package org.commerce.common;

import java.util.List;

/** 拆分任务的状态布局。组内顺序同样属于 checkpoint 契约，发布后不可随意调整。 */
public enum MetricGroup {
    AMOUNT(
            "amount",
            Metric.CREATED_ORDERS,
            Metric.CANCELLED_ORDERS,
            Metric.PAID_ORDERS,
            Metric.REFUND_REQUESTS,
            Metric.PAID_QUANTITY,
            Metric.CREATED_GOODS_CENT,
            Metric.PAID_GOODS_CENT,
            Metric.REFUND_GOODS_CENT,
            Metric.NET_PAID_GOODS_CENT),
    DISTINCT("distinct", Metric.PAID_USERS, Metric.REFUND_ORDERS);

    private final String statePrefix;
    private final List<Metric> metrics;

    MetricGroup(String statePrefix, Metric... metrics) {
        this.statePrefix = statePrefix;
        this.metrics = List.of(metrics);
    }

    public String statePrefix() {
        return statePrefix;
    }

    public List<Metric> metrics() {
        return metrics;
    }
}
