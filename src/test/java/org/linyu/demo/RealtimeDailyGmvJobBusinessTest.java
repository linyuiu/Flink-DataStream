package org.linyu.demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.linyu.map.GmvDeltaRealTime;
import org.linyu.map.OrderDetailRealTime;
import org.linyu.transform.OrderContributionProcessFunction;
import org.linyu.transform.OrderJsonProcessFunction;
import org.linyu.validation.OrderSnapshotValidator;
import org.linyu.validation.OrderValidationException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link RealtimeDailyGmvJob} 的核心业务测试。
 *
 * <p>测试只运行“Kafka value JSON 解析 -> 订单状态去重 -> GMV 变化量计算”这条纯业务链路，
 * 不连接真实 Kafka 和 Doris，避免单元测试依赖外部环境。</p>
 */
class RealtimeDailyGmvJobBusinessTest {

    @Test
    void paidThenPartialAndFullRefundShouldReduceGmvToZero() throws Exception {
        List<GmvDeltaRealTime> deltas = runContributionFlow(
                orderJson(
                        "order-1", "389.16", "0.00", "PAID",
                        "2026-08-11 07:44:00", "2026-08-11 16:03:00"
                ),
                orderJson(
                        "order-1", "389.16", "100.00", "PART_REFUNDED",
                        "2026-08-11 07:44:00", "2026-08-11 16:04:00"
                ),
                orderJson(
                        "order-1", "389.16", "389.16", "REFUNDED",
                        "2026-08-11 07:44:00", "2026-08-11 16:05:00"
                )
        );

        assertEquals(3, deltas.size());
        assertDelta(deltas.get(0), "2026-08-11", "389.16", "order-1");
        assertDelta(deltas.get(1), "2026-08-11", "-100.00", "order-1");
        assertDelta(deltas.get(2), "2026-08-11", "-289.16", "order-1");
        assertAmount("0.00", totalsByDate(deltas).get("2026-08-11"));
    }

    @Test
    void duplicateAndOlderOrderVersionsShouldNotChangeGmv() throws Exception {
        List<GmvDeltaRealTime> deltas = runContributionFlow(
                orderJson(
                        "order-2", "100.00", "0.00", "PAID",
                        "2026-08-11 08:00:00", "2026-08-11 16:05:00"
                ),
                // 相同 update_time：重复版本，即使内容不同也应忽略。
                orderJson(
                        "order-2", "100.00", "40.00", "PART_REFUNDED",
                        "2026-08-11 08:00:00", "2026-08-11 16:05:00"
                ),
                // 更早的 update_time：迟到旧版本应忽略。
                orderJson(
                        "order-2", "100.00", "100.00", "REFUNDED",
                        "2026-08-11 08:00:00", "2026-08-11 16:04:00"
                )
        );

        assertEquals(1, deltas.size());
        assertDelta(deltas.get(0), "2026-08-11", "100.00", "order-2");
    }

    @Test
    void businessDateCorrectionShouldRevokeOldDateBeforeAddingNewDate() throws Exception {
        List<GmvDeltaRealTime> deltas = runContributionFlow(
                orderJson(
                        "order-3", "100.00", "0.00", "PAID",
                        "2026-08-11 23:59:00", "2026-08-12 00:01:00"
                ),
                orderJson(
                        "order-3", "100.00", "20.00", "PART_REFUNDED",
                        "2026-08-12 00:01:00", "2026-08-12 00:02:00"
                )
        );

        assertEquals(3, deltas.size());
        assertDelta(deltas.get(0), "2026-08-11", "100.00", "order-3");
        assertDelta(deltas.get(1), "2026-08-11", "-100.00", "order-3");
        assertDelta(deltas.get(2), "2026-08-12", "80.00", "order-3");

        Map<String, BigDecimal> totals = totalsByDate(deltas);
        assertAmount("0.00", totals.get("2026-08-11"));
        assertAmount("80.00", totals.get("2026-08-12"));
    }

    @Test
    void refundSnapshotWithoutPayAmountShouldFailBusinessValidation() {
        OrderDetailRealTime refund = order(
                "order-4", null, "50.00", "PART_REFUNDED",
                "2026-08-11 08:00:00", "2026-08-11 16:06:00"
        );

        OrderValidationException exception = assertThrows(
                OrderValidationException.class,
                () -> OrderSnapshotValidator.validate(refund)
        );

        assertEquals(
                "PART_REFUNDED状态下pay_amount不能为空",
                exception.getMessage()
        );
    }

    @Test
    void malformedOrBusinessInvalidKafkaValueShouldNotPolluteOrderState() throws Exception {
        String malformedJson = "{\"order_id\":\"order-5\",\"pay_amount\":," +
                "\"refund_amount\":50.00,\"order_status\":\"PART_REFUNDED\"}";

        List<GmvDeltaRealTime> deltas = runContributionFlow(
                malformedJson,
                orderJson(
                        "order-5", null, "50.00", "PART_REFUNDED",
                        "2026-08-11 08:00:00", "2026-08-11 16:06:00"
                ),
                orderJson(
                        "order-5", "100.00", "0.00", "PAID",
                        "2026-08-11 08:00:00", "2026-08-11 16:07:00"
                )
        );

        // 前两条分别在 JSON 校验和业务校验阶段被拒绝，最后一条应按首次有效状态贡献 100。
        assertEquals(1, deltas.size());
        assertDelta(deltas.get(0), "2026-08-11", "100.00", "order-5");
    }

    private static List<GmvDeltaRealTime> runContributionFlow(String... kafkaValues)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderDetailRealTime> orders = env
                .fromElements(kafkaValues)
                .process(new OrderJsonProcessFunction());

        SingleOutputStreamOperator<GmvDeltaRealTime> deltas = orders
                .keyBy(order -> order.orderId)
                .process(new OrderContributionProcessFunction());

        List<GmvDeltaRealTime> result = new ArrayList<>();
        try (CloseableIterator<GmvDeltaRealTime> iterator = deltas.executeAndCollect()) {
            iterator.forEachRemaining(result::add);
        }
        return result;
    }

    private static String orderJson(
            String orderId,
            String payAmount,
            String refundAmount,
            String status,
            String payTime,
            String updateTime) {
        return "{"
                + "\"order_id\":\"" + orderId + "\","
                + "\"pay_amount\":" + jsonNumber(payAmount) + ","
                + "\"refund_amount\":" + jsonNumber(refundAmount) + ","
                + "\"order_status\":\"" + status + "\","
                + "\"pay_time\":\"" + payTime + "\","
                + "\"update_time\":\"" + updateTime + "\""
                + "}";
    }

    private static String jsonNumber(String value) {
        return value == null ? "null" : value;
    }

    private static OrderDetailRealTime order(
            String orderId,
            String payAmount,
            String refundAmount,
            String status,
            String payTime,
            String updateTime) {
        OrderDetailRealTime order = new OrderDetailRealTime();
        order.orderId = orderId;
        order.payAmount = decimal(payAmount);
        order.refundAmount = decimal(refundAmount);
        order.orderStatus = status;
        order.payTime = payTime;
        order.updateTime = updateTime;
        return order;
    }

    private static BigDecimal decimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }

    private static Map<String, BigDecimal> totalsByDate(List<GmvDeltaRealTime> deltas) {
        Map<String, BigDecimal> totals = new HashMap<>();
        for (GmvDeltaRealTime delta : deltas) {
            totals.merge(delta.bizDate, delta.deltaAmount, BigDecimal::add);
        }
        return totals;
    }

    private static void assertDelta(
            GmvDeltaRealTime actual,
            String expectedDate,
            String expectedAmount,
            String expectedOrderId) {
        assertEquals(expectedDate, actual.bizDate);
        assertAmount(expectedAmount, actual.deltaAmount);
        assertEquals(expectedOrderId, actual.orderId);
    }

    private static void assertAmount(String expected, BigDecimal actual) {
        assertEquals(
                0,
                new BigDecimal(expected).compareTo(actual),
                () -> "expected amount " + expected + " but was " + actual
        );
    }
}
