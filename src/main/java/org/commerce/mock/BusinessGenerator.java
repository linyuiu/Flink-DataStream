package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.commerce.common.Json;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

import java.util.*;

/** 纯业务场景生成器：只构造事务计划，不连接数据库。 金额分摊和支付/退款成功状态由这里模拟的业务服务负责，Flink 不推测业务是否成功。 */
public final class BusinessGenerator {
    private static final long PAYMENT_DELAY_MS = 30_000;
    private static final long REFUND_DELAY_MS = 60_000;
    private static final long OLD_ORDER_AGE_MS = 14L * 86_400_000;

    private BusinessGenerator() {}

    public static List<TransactionPlan> generate(
            String orderId, Scenario scenario, long createdAt, long goodsCent, Random random) {
        if (goodsCent < 200) {
            throw new IllegalArgumentException("At least 200 cents per two-line demo order");
        }

        // 1. 创建子订单和两条购买明细，Outbox 与业务数据位于同一个事务计划。
        TradeEvent created = createOrderEvent(orderId, createdAt, goodsCent, random);
        ObjectNode orderRow = createOrderRow(created);
        TransactionPlan creation = new TransactionPlan().insert("trade_order", orderRow);
        addOrderLines(creation, created, random);
        List<TransactionPlan> plans = new ArrayList<>();
        plans.add(creation.event(created));

        // 2. 取消与支付是互斥分支；失败支付仅产生尝试记录，不产生成功支付流水。
        if (scenario == Scenario.CANCELLED) {
            addCancellation(plans, created, orderRow);
            return validatePlans(plans);
        }

        long payableCent = orderRow.path("payable_cent").asLong();
        if (scenario == Scenario.PAYMENT_FAILED || scenario == Scenario.RETRY_PAID) {
            addFailedPaymentAttempt(plans, orderId, payableCent);
            if (scenario == Scenario.PAYMENT_FAILED) {
                return validatePlans(plans);
            }
        }

        // 3. 成功支付包含尝试状态、成功流水、订单状态、商品分摊与支付事件。
        TradeEvent paid = addSuccessfulPayment(plans, created, orderRow);
        if (scenario == Scenario.PAID || scenario == Scenario.RETRY_PAID) {
            return validatePlans(plans);
        }

        // 4. 在原支付分摊基础上生成独立退款请求，不修改原支付成功流水。
        addRefundScenario(plans, paid, scenario);
        return validatePlans(plans);
    }

    private static TradeEvent createOrderEvent(
            String orderId, long createdAt, long goodsCent, Random random) {
        // 保持随机抽样顺序稳定：同一 seed 应产生同样的订单和事件。
        String checkoutId = "CK-" + orderId;
        String userId = "U" + random.nextInt(500000);
        String shopId = "S" + (random.nextInt(Catalog.SHOPS) + 1);

        TradeEvent created = new TradeEvent();
        created.eventId = "create-" + orderId;
        created.eventType = "ORDER_CREATED";
        created.businessId = orderId;
        created.orderId = orderId;
        created.checkoutId = checkoutId;
        created.userId = userId;
        created.shopId = shopId;

        boolean shanghai = random.nextBoolean();
        created.provinceId = shanghai ? "310000" : "330000";
        created.cityId = shanghai ? "310100" : "330100";
        created.districtId = shanghai ? "310115" : "330106";
        created.currency = "CNY";
        created.occurredAt = createdAt;
        created.goodsCent = goodsCent;
        return created;
    }

    private static ObjectNode createOrderRow(TradeEvent created) {
        ObjectNode row =
                newRow(created.orderId)
                        .put("checkout_id", created.checkoutId)
                        .put("user_id", created.userId)
                        .put("shop_id", created.shopId)
                        .put("province_id", created.provinceId)
                        .put("city_id", created.cityId)
                        .put("district_id", created.districtId)
                        .put("created_at", created.occurredAt)
                        .put("cancelled_at", 0)
                        .put("status", "CREATED")
                        .put("currency", "CNY")
                        .put("goods_cent", created.goodsCent)
                        .put("freight_cent", created.goodsCent >= 5000 ? 0 : 500)
                        .put("line_count", 2);
        row.put("payable_cent", created.goodsCent + row.path("freight_cent").asLong());
        return row;
    }

    private static void addOrderLines(TransactionPlan creation, TradeEvent created, Random random) {
        // 两行按 60%/40% 分摊，余分留给第二行，确保订单头与明细金额完全相等。
        long firstLineCent = created.goodsCent * 3 / 5;
        long[] lineAmounts = {firstLineCent, created.goodsCent - firstLineCent};
        for (int lineIndex = 0; lineIndex < lineAmounts.length; lineIndex++) {
            // 20% 概率集中到 SKU1，用来保留热点商品的测试流量。
            int skuNumber = random.nextInt(5) == 0 ? 1 : random.nextInt(Catalog.SKUS) + 1;
            TradeEvent.Line line = new TradeEvent.Line();
            line.orderLineId = created.orderId + "-L" + lineIndex;
            line.skuId = "SKU" + skuNumber;
            line.productId = Catalog.product(skuNumber);
            line.categoryId = Catalog.category(skuNumber);
            line.brandId = Catalog.brand(skuNumber);
            line.amountCent = lineAmounts[lineIndex];
            line.quantity = 1;
            created.lines.add(line);

            long discountCent = random.nextBoolean() ? 100 : 0;
            creation.insert(
                    "trade_order_line",
                    newRow(line.orderLineId)
                            .put("order_id", created.orderId)
                            .put("sku_id", line.skuId)
                            .put("product_id", line.productId)
                            .put("category_id", line.categoryId)
                            .put("brand_id", line.brandId)
                            .put("quantity", 1)
                            .put("unit_price_cent", line.amountCent + discountCent)
                            .put("discount_cent", discountCent)
                            .put("payable_cent", line.amountCent));
        }
    }

    private static void addCancellation(
            List<TransactionPlan> plans, TradeEvent created, ObjectNode orderRow) {
        long cancelledAt = created.occurredAt + 60_000;
        TradeEvent cancelled =
                copyAsEvent(
                        created,
                        "cancel-" + created.orderId,
                        "ORDER_CANCELLED",
                        created.orderId,
                        cancelledAt,
                        0);
        plans.add(
                new TransactionPlan()
                        .update(
                                "trade_order",
                                nextVersion(orderRow)
                                        .put("status", "CANCELLED")
                                        .put("cancelled_at", cancelledAt))
                        .event(cancelled));
    }

    private static void addFailedPaymentAttempt(
            List<TransactionPlan> plans, String orderId, long payableCent) {
        ObjectNode attempt = paymentAttempt(orderId + "-A0", orderId, payableCent);
        plans.add(new TransactionPlan().insert("pay_attempt", attempt));
        plans.add(
                new TransactionPlan()
                        .update(
                                "pay_attempt",
                                nextVersion(attempt)
                                        .put("status", "FAILED")
                                        .put("failure_code", "DECLINED")));
    }

    private static TradeEvent addSuccessfulPayment(
            List<TransactionPlan> plans, TradeEvent created, ObjectNode orderRow) {
        String orderId = created.orderId;
        String paymentId = orderId + "-PAY";
        long payableCent = orderRow.path("payable_cent").asLong();
        long paidAt = created.occurredAt + PAYMENT_DELAY_MS;

        ObjectNode attempt = paymentAttempt(orderId + "-A1", orderId, payableCent);
        plans.add(new TransactionPlan().insert("pay_attempt", attempt));
        TradeEvent paid =
                copyAsEvent(
                        created, "pay-" + orderId, "PAYMENT_SUCCEEDED", paymentId, paidAt, paidAt);

        TransactionPlan payment =
                new TransactionPlan()
                        .update("pay_attempt", nextVersion(attempt).put("status", "SUCCEEDED"))
                        .insert(
                                "pay_ledger",
                                newRow(paymentId)
                                        .put("order_id", orderId)
                                        .put("attempt_id", orderId + "-A1")
                                        .put("channel_txn_id", "CH-" + orderId)
                                        .put("paid_at", paidAt)
                                        .put("amount_cent", payableCent))
                        .update("trade_order", nextVersion(orderRow).put("status", "PAID"))
                        .event(paid);
        for (TradeEvent.Line line : paid.lines) {
            String lineSuffix = line.orderLineId.substring(line.orderLineId.lastIndexOf('-') + 1);
            payment.insert(
                    "pay_allocation",
                    newRow(paymentId + "-" + lineSuffix)
                            .put("payment_id", paymentId)
                            .put("order_line_id", line.orderLineId)
                            .put("amount_cent", line.amountCent));
        }
        plans.add(payment);
        return paid;
    }

    private static void addRefundScenario(
            List<TransactionPlan> plans, TradeEvent paid, Scenario scenario) {
        long refundAt =
                paid.paidAt
                        + (scenario == Scenario.OLD_ORDER_REFUND
                                ? OLD_ORDER_AGE_MS
                                : REFUND_DELAY_MS);
        long firstLineCent = paid.lines.get(0).amountCent;
        long firstRefundCent = firstLineCent / 3;

        addRefund(
                plans,
                paid,
                paid.orderId + "-R1",
                new long[] {firstRefundCent, 0},
                refundAt,
                scenario != Scenario.REFUND_FAILED);
        if (scenario == Scenario.MULTIPLE_REFUNDS) {
            addRefund(
                    plans,
                    paid,
                    paid.orderId + "-R2",
                    new long[] {firstLineCent / 4, 0},
                    refundAt + 30_000,
                    true);
        }
        if (scenario == Scenario.FULL_GOODS_REFUND) {
            // 第二次退完剩余商品金额；此处不退运费，也不代表实物已退回。
            addRefund(
                    plans,
                    paid,
                    paid.orderId + "-R2",
                    new long[] {firstLineCent - firstRefundCent, paid.goodsCent - firstLineCent},
                    refundAt + 30_000,
                    true);
        }
    }

    private static void addRefund(
            List<TransactionPlan> plans,
            TradeEvent paid,
            String refundId,
            long[] amounts,
            long succeededAt,
            boolean succeeded) {
        ObjectNode request =
                newRow(refundId)
                        .put("order_id", paid.orderId)
                        .put("payment_id", paid.businessId)
                        .put("amount_cent", amounts[0] + amounts[1])
                        .put("status", "APPLIED")
                        .put("succeeded_at", 0);
        TransactionPlan application = new TransactionPlan().insert("refund_request", request);
        TradeEvent refunded =
                copyAsEvent(
                        paid,
                        "refund-" + refundId,
                        "REFUND_SUCCEEDED",
                        refundId,
                        succeededAt,
                        paid.paidAt);
        refunded.lines.clear();
        refunded.goodsCent = 0;

        for (int lineIndex = 0; lineIndex < amounts.length; lineIndex++) {
            if (amounts[lineIndex] <= 0) {
                continue;
            }
            TradeEvent.Line originalLine = paid.lines.get(lineIndex);
            TradeEvent.Line refundedLine = copyLine(originalLine);
            refundedLine.amountCent = amounts[lineIndex];
            refundedLine.quantity = 0;
            refunded.lines.add(refundedLine);
            refunded.goodsCent = Math.addExact(refunded.goodsCent, amounts[lineIndex]);
            application.insert(
                    "refund_allocation",
                    newRow(refundId + "-L" + lineIndex)
                            .put("refund_id", refundId)
                            .put("order_line_id", originalLine.orderLineId)
                            .put("amount_cent", amounts[lineIndex]));
        }
        plans.add(application);

        ObjectNode processing = nextVersion(request).put("status", "PROCESSING");
        plans.add(new TransactionPlan().update("refund_request", processing));
        TransactionPlan completion =
                new TransactionPlan()
                        .update(
                                "refund_request",
                                nextVersion(processing)
                                        .put("status", succeeded ? "SUCCEEDED" : "FAILED")
                                        .put("succeeded_at", succeeded ? succeededAt : 0));
        // 只有渠道确认退款成功，才发布会影响 GMV 的事实。
        if (succeeded) {
            completion.event(refunded);
        }
        plans.add(completion);
    }

    private static ObjectNode paymentAttempt(String attemptId, String orderId, long amountCent) {
        return newRow(attemptId)
                .put("order_id", orderId)
                .put("amount_cent", amountCent)
                .put("status", "PENDING")
                .put("failure_code", "");
    }

    private static ObjectNode newRow(String id) {
        return Json.MAPPER.createObjectNode().put("id", id).put("version", 1);
    }

    private static ObjectNode nextVersion(ObjectNode previous) {
        return previous.deepCopy().put("version", previous.path("version").asLong() + 1);
    }

    private static TradeEvent.Line copyLine(TradeEvent.Line line) {
        try {
            return Json.MAPPER.readValue(Json.write(line), TradeEvent.Line.class);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static TradeEvent copyAsEvent(
            TradeEvent original,
            String eventId,
            String eventType,
            String businessId,
            long occurredAt,
            long paidAt) {
        try {
            TradeEvent event = Json.MAPPER.readValue(Json.write(original), TradeEvent.class);
            event.eventId = eventId;
            event.eventType = eventType;
            event.businessId = businessId;
            event.occurredAt = occurredAt;
            event.paidAt = paidAt;
            return event;
        } catch (java.io.IOException exception) {
            throw new IllegalStateException(exception);
        }
    }

    /** 业务侧守恒检查：同一订单行多次成功退款的累计额不得超过原支付分摊。 */
    private static List<TransactionPlan> validatePlans(List<TransactionPlan> plans) {
        Map<String, Long> paidByLine = new HashMap<>();
        Map<String, Long> refundedByLine = new HashMap<>();
        for (TransactionPlan plan : plans) {
            if (plan.event == null) {
                continue;
            }
            EventContract.validate(plan.event);
            for (TradeEvent.Line line : plan.event.lines) {
                if (plan.event.eventType.equals("PAYMENT_SUCCEEDED")) {
                    paidByLine.put(line.orderLineId, line.amountCent);
                }
                if (plan.event.eventType.equals("REFUND_SUCCEEDED")) {
                    long totalRefunded =
                            Math.addExact(
                                    refundedByLine.getOrDefault(line.orderLineId, 0L),
                                    line.amountCent);
                    EventContract.require(
                            totalRefunded <= paidByLine.getOrDefault(line.orderLineId, 0L),
                            "Service over-refund");
                    refundedByLine.put(line.orderLineId, totalRefunded);
                }
            }
        }
        return plans;
    }
}
