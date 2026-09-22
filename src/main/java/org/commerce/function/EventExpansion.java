package org.commerce.function;

import org.commerce.common.Contract;
import org.commerce.common.Metric;
import org.commerce.model.MetricDelta;
import org.commerce.model.TradeEvent;

import java.util.*;

/** 把一个订单事实展开为多维度增量，不保存 Flink 状态。 */
public final class EventExpansion {
    private EventExpansion() {}

    public static List<MetricDelta> expand(
            TradeEvent event, int globalShards, int dimensionShards, int entityShards) {
        // 先在本订单内部合并同维度明细：两行同品牌商品的金额相加，但品牌订单数只能加 1。
        Map<String, DimensionTotals> totalsByDimension = groupOrderLines(event);
        List<MetricDelta> deltas = new ArrayList<>();

        totalsByDimension.forEach(
                (dimensionKey, totals) -> {
                    String[] dimension = dimensionKey.split("\\|");
                    int shardCount =
                            shardCount(dimension[0], globalShards, dimensionShards, entityShards);
                    MetricDelta delta =
                            createDelta(
                                    Horizons.date(event.occurredAt),
                                    dimension,
                                    event.orderId,
                                    shardCount);

                    switch (event.eventType) {
                        case "ORDER_CREATED":
                            delta.set(Metric.CREATED_ORDERS, 1);
                            delta.set(Metric.CREATED_GOODS_CENT, totals.goodsCent);
                            break;
                        case "ORDER_CANCELLED":
                            delta.set(Metric.CANCELLED_ORDERS, 1);
                            break;
                        case "PAYMENT_SUCCEEDED":
                            addPaymentDeltas(event, dimension, totals, shardCount, delta, deltas);
                            break;
                        case "REFUND_SUCCEEDED":
                            addRefundDeltas(event, dimension, totals, shardCount, delta, deltas);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown event type");
                    }
                    deltas.add(delta);
                });
        return deltas;
    }

    private static Map<String, DimensionTotals> groupOrderLines(TradeEvent event) {
        Map<String, DimensionTotals> groups = new HashMap<>();
        for (TradeEvent.Line line : event.lines) {
            String[][] dimensions = {
                {"ALL", "ALL"},
                {"SHOP", event.shopId},
                {"PRODUCT", line.productId},
                {"SKU", line.skuId},
                {"CATEGORY", line.categoryId},
                {"BRAND", line.brandId},
                {"PROVINCE", event.provinceId},
                {"CITY", event.cityId},
                {"DISTRICT", event.districtId}
            };
            for (String[] dimension : dimensions) {
                String key = dimension[0] + "|" + dimension[1];
                DimensionTotals totals =
                        groups.computeIfAbsent(key, ignored -> new DimensionTotals());
                totals.goodsCent = Math.addExact(totals.goodsCent, line.amountCent);
                totals.quantity = Math.addExact(totals.quantity, line.quantity);
            }
        }
        return groups;
    }

    private static void addPaymentDeltas(
            TradeEvent event,
            String[] dimension,
            DimensionTotals totals,
            int shardCount,
            MetricDelta delta,
            List<MetricDelta> output) {
        delta.set(Metric.PAID_ORDERS, 1);
        delta.set(Metric.PAID_QUANTITY, totals.quantity);
        delta.set(Metric.PAID_GOODS_CENT, totals.goodsCent);
        delta.set(Metric.NET_PAID_GOODS_CENT, totals.goodsCent);

        // 同一用户可多次支付，先交给精确去重算子，不能每单直接增加一次人数。
        MetricDelta userRequest = createDelta(delta.date, dimension, event.userId, shardCount);
        userRequest.distinctKind = "PAID_USER";
        userRequest.distinctEntity = event.userId;
        output.add(userRequest);
    }

    private static void addRefundDeltas(
            TradeEvent event,
            String[] dimension,
            DimensionTotals totals,
            int shardCount,
            MetricDelta delta,
            List<MetricDelta> output) {
        // 第一条金额增量归退款发生日；退款笔数与退款订单数是不同指标。
        delta.set(Metric.REFUND_REQUESTS, 1);
        delta.set(Metric.REFUND_GOODS_CENT, totals.goodsCent);

        MetricDelta orderRequest = createDelta(delta.date, dimension, event.orderId, shardCount);
        orderRequest.distinctKind = "REFUND_ORDER";
        orderRequest.distinctEntity = event.orderId;
        output.add(orderRequest);

        // 第二条金额增量归原支付日，只回扣净 GMV，不改退款前的支付 GMV。
        MetricDelta correction =
                createDelta(Horizons.date(event.paidAt), dimension, event.orderId, shardCount);
        correction.set(Metric.NET_PAID_GOODS_CENT, -totals.goodsCent);
        output.add(correction);
    }

    private static int shardCount(
            String dimensionType, int globalShards, int dimensionShards, int entityShards) {
        if (dimensionType.equals("ALL")) {
            return globalShards;
        }
        if (dimensionType.equals("SKU") || dimensionType.equals("PRODUCT")) {
            return entityShards;
        }
        return dimensionShards;
    }

    private static MetricDelta createDelta(
            String date, String[] dimension, String entityId, int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("Invalid shard count");
        }
        MetricDelta delta = new MetricDelta();
        delta.date = date;
        delta.dimensionType = dimension[0];
        delta.dimensionId = dimension[1];
        delta.partitionEntity = entityId;
        delta.shard = Math.floorMod(entityId.hashCode(), shardCount);
        delta.values = new long[Contract.METRICS.length];
        return delta;
    }

    /** 仅在一次方法调用中使用的局部小计，不进入 Flink 状态或 Kafka。 */
    private static final class DimensionTotals {
        private long goodsCent;
        private long quantity;
    }
}
