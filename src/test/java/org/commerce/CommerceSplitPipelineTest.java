package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CloseableIterator;
import org.commerce.common.*;
import org.commerce.config.CommerceConfig;
import org.commerce.function.Horizons;
import org.commerce.job.TradePipelines;
import org.commerce.model.TradeEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.*;
import java.util.*;

/**
 * 在本地 Flink MiniCluster 跑真实共享算子、shuffle、RocksDB 和处理时间 timer。 测试将 DWD 边界直接连接；不模拟 Kafka/Doris
 * 事务，也不声称验证了生产吞吐。
 */
class CommerceSplitPipelineTest {
    private static final String[] COLUMNS = {
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
    private final long baseTime =
            LocalDate.now(Contract.ZONE)
                    .minusDays(2)
                    .atTime(12, 0)
                    .atZone(Contract.ZONE)
                    .toInstant()
                    .toEpochMilli();

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    @Timeout(60)
    void allElevenMetricsRemainCorrectAcrossEighteenDimensionRows(int parallelism)
            throws Exception {
        List<TradeEvent> events = new ArrayList<>();
        TradeEvent first = order("O1", "U1", "S1", baseTime, 1, 2);
        TradeEvent second = order("O2", "U1", "S1", baseTime, 3);
        TradeEvent third = order("O3", "U2", "S2", baseTime, 1);
        TradeEvent cancelled = order("O4", "U3", "S2", baseTime, 3);
        TradeEvent paymentFailed = order("O5", "U4", "S1", baseTime, 3);
        TradeEvent refundFailed = order("O6", "U5", "S2", baseTime, 3);
        Collections.addAll(
                events,
                first,
                second,
                third,
                cancelled,
                paymentFailed,
                refundFailed,
                paid(first),
                paid(second),
                paid(third),
                paid(refundFailed));
        TradeEvent cancel = copy(cancelled);
        cancel.eventId = "cancel-O4";
        cancel.eventType = "ORDER_CANCELLED";
        cancel.occurredAt += 120000;
        events.add(cancel);
        // 支付失败没有支付成功事实；退款失败也没有退款成功事实。
        events.add(refund(first, "R1", 1000, baseTime + 180000));
        events.add(refund(first, "R2", 2000, baseTime + 240000));
        events.add(refund(third, "R3", 3000, baseTime + 180000));

        Map<String, long[]> expected = new HashMap<>();
        String day = Horizons.date(baseTime);
        long[] all = {6, 1, 4, 3, 2, 3, 5, 21000, 15000, 6000, 9000};
        long[] shop1 = {3, 0, 2, 1, 1, 2, 3, 12000, 9000, 3000, 6000};
        long[] shop2 = {3, 1, 2, 2, 1, 1, 2, 9000, 6000, 3000, 3000};
        long[] product1 = {2, 0, 2, 2, 2, 3, 3, 9000, 9000, 6000, 3000};
        long[] product2 = {4, 1, 2, 2, 0, 0, 2, 12000, 6000, 0, 6000};
        addExpected(expected, day, all, "ALL|ALL");
        addExpected(
                expected,
                day,
                shop1,
                "SHOP|S1",
                "PROVINCE|310000",
                "CITY|310100",
                "DISTRICT|310115");
        addExpected(
                expected,
                day,
                shop2,
                "SHOP|S2",
                "PROVINCE|330000",
                "CITY|330100",
                "DISTRICT|330106");
        addExpected(expected, day, product1, "PRODUCT|P1", "CATEGORY|C1", "BRAND|B1");
        addExpected(expected, day, product2, "PRODUCT|P2", "CATEGORY|C2", "BRAND|B2", "SKU|SKU3");
        addExpected(
                expected, day, new long[] {2, 0, 2, 2, 2, 3, 2, 6000, 6000, 6000, 0}, "SKU|SKU1");
        addExpected(
                expected, day, new long[] {1, 0, 1, 1, 0, 0, 1, 3000, 3000, 0, 3000}, "SKU|SKU2");

        List<String> input = envelopes(events);
        input.addAll(envelopes(events)); // 全量重放并打乱，先到退款也必须最终守恒。
        Collections.shuffle(input, new Random(2026));
        Observed observed = run(input, parallelism, expected, true, events.size(), 0, 0);
        assertEquals(18, observed.rows().size());
        assertTrue(observed.amountSnapshots > 0 && observed.distinctSnapshots > 0);
    }

    @Test
    @Timeout(60)
    void refundDayAndOriginalPaymentDayAreNotMixed() throws Exception {
        TradeEvent yesterday = order("CROSS1", "same-user", "S1", baseTime, 1, 2);
        TradeEvent today = order("CROSS2", "same-user", "S1", baseTime + 86400000L, 3);
        var events =
                List.of(
                        yesterday,
                        paid(yesterday),
                        today,
                        paid(today),
                        refund(yesterday, "CROSS-R", 3000, baseTime + 86400000L + 180000));
        Map<String, long[]> expected = new HashMap<>();
        addExpected(
                expected,
                Horizons.date(baseTime),
                new long[] {1, 0, 1, 1, 0, 0, 2, 6000, 6000, 0, 3000},
                "ALL|ALL");
        addExpected(
                expected,
                Horizons.date(baseTime + 86400000L),
                new long[] {1, 0, 1, 1, 1, 1, 1, 3000, 3000, 3000, 3000},
                "ALL|ALL");
        List<String> input = envelopes(events);
        Collections.reverse(input);
        run(input, 2, expected, false, 5, 0, 0);
    }

    @Test
    @Timeout(60)
    void invalidConflictingAndExpiredMessagesAreObservableWithoutPollutingMetrics()
            throws Exception {
        TradeEvent created = order("VALID", "U", "S1", baseTime, 1);
        TradeEvent payment = paid(created);
        List<String> input = envelopes(List.of(created, payment, created, payment));
        input.add("{broken-json");
        input.add(envelope(created, "u"));
        TradeEvent conflict = copy(payment);
        conflict.userId = "different-user";
        input.add(envelope(conflict, "c"));
        TradeEvent badAmount = order("BAD", "U", "S1", baseTime, 1);
        badAmount.goodsCent++;
        input.add(envelope(badAmount, "c"));
        TradeEvent future = order("FUTURE", "U", "S1", System.currentTimeMillis() + 3600000, 1);
        input.add(envelope(future, "c"));
        TradeEvent expired = order("OLD", "U", "S1", baseTime - 20L * 86400000, 1);
        input.add(envelope(expired, "c"));

        Map<String, long[]> expected = new HashMap<>();
        addExpected(
                expected,
                Horizons.date(baseTime),
                new long[] {1, 0, 1, 1, 0, 0, 1, 3000, 3000, 0, 3000},
                "ALL|ALL");
        Observed observed = run(input, 1, expected, false, 2, 5, 1);
        assertTrue(
                observed.quality.stream()
                        .anyMatch(row -> row.path("stage").asText().equals("event-conflict")));
        assertEquals("event-horizon", observed.repair.get(0).path("stage").asText());
    }

    @Test
    @Timeout(60)
    void oldPaymentCorrectionGoesToRepairWhileCurrentRefundStillCounts() throws Exception {
        TradeEvent oldOrder = order("HISTORIC", "U", "S1", baseTime - 100L * 86400000, 1);
        TradeEvent refund = refund(oldOrder, "HIST-R", 1000, baseTime);
        Map<String, long[]> expected = new HashMap<>();
        addExpected(
                expected,
                Horizons.date(baseTime),
                new long[] {0, 0, 0, 0, 1, 1, 0, 0, 0, 1000, 0},
                "ALL|ALL");
        Observed observed = run(envelopes(List.of(refund)), 2, expected, false, 1, 0, 9);
        assertTrue(
                observed.repair.stream()
                        .allMatch(row -> row.path("stage").asText().equals("metric-horizon")));
        assertTrue(
                observed.rows().keySet().stream()
                        .allMatch(key -> key.startsWith(Horizons.date(baseTime))));
    }

    private static Observed run(
            List<String> input,
            int parallelism,
            Map<String, long[]> expected,
            boolean exactRows,
            int acceptedCount,
            int qualityCount,
            int repairCount)
            throws Exception {
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.setParallelism(parallelism);
        environment.setMaxParallelism(128);
        environment.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        if (parallelism == 4) {
            environment.getConfig().enableObjectReuse();
        }
        Properties properties = new Properties();
        properties.setProperty("online.event.days", "7");
        properties.setProperty("online.metric.days", "93");
        properties.setProperty("online.distinct.metric.days", "9");
        properties.setProperty("flush.interval.ms", "20");
        properties.setProperty("shards.global", "32");
        properties.setProperty("shards.dimensions", "8");
        properties.setProperty("shards.entities", "4");
        CommerceConfig config = new CommerceConfig(properties);
        var source = environment.addSource(new HoldingSource(input)).setParallelism(1);
        var facts = TradePipelines.facts(source, config);
        var amounts = TradePipelines.metrics(facts.records, config, MetricGroup.AMOUNT);
        var distincts = TradePipelines.metrics(facts.records, config, MetricGroup.DISTINCT);

        var collected =
                amounts.records
                        .map(row -> "AMOUNT\t" + row)
                        .returns(String.class)
                        .union(
                                distincts
                                        .records
                                        .map(row -> "DISTINCT\t" + row)
                                        .returns(String.class),
                                facts.records.map(row -> "FACT\t" + row).returns(String.class),
                                facts.quality
                                        .union(amounts.quality, distincts.quality)
                                        .map(row -> "QUALITY\t" + row)
                                        .returns(String.class),
                                facts.repair
                                        .union(amounts.repair, distincts.repair)
                                        .map(row -> "REPAIR\t" + row)
                                        .returns(String.class))
                        .transform("test-drain-signal", Types.STRING, new DrainSignalOperator())
                        .setParallelism(1);
        Observed observed = new Observed();
        try (CloseableIterator<String> iterator = collected.executeAndCollect()) {
            while (iterator.hasNext()) {
                observed.accept(iterator.next());
                if (!observed.drained) {
                    continue;
                }
                assertEquals(acceptedCount, observed.accepted, "公共事实去重输出数");
                assertEquals(qualityCount, observed.quality.size(), "质量分流数");
                assertEquals(repairCount, observed.repair.size(), "修复分流数");
                if (observed.matches(expected, exactRows)) {
                    return observed;
                }
            }
        }
        fail("未得到完整指标结果：" + observed.rows().keySet());
        return observed;
    }

    /** 与 Doris 相同：先按各自表主键/序列覆盖分片绝对值，再 UNION ALL + SUM，不累加旧版本。 */
    private static final class Observed {
        final Map<String, JsonNode> latest = new HashMap<>();
        final List<JsonNode> quality = new ArrayList<>(), repair = new ArrayList<>();
        int accepted, amountSnapshots, distinctSnapshots;
        boolean drained;

        void accept(String value) {
            if (value.equals("DRAINED")) {
                drained = true;
                return;
            }
            String[] message = value.split("\t", 2);
            JsonNode row = Json.object(message[1]);
            switch (message[0]) {
                case "FACT":
                    accepted++;
                    return;
                case "QUALITY":
                    quality.add(row);
                    return;
                case "REPAIR":
                    repair.add(row);
                    return;
                case "AMOUNT":
                    amountSnapshots++;
                    break;
                case "DISTINCT":
                    distinctSnapshots++;
                    break;
                default:
                    throw new AssertionError("Unexpected channel");
            }
            MetricGroup group =
                    message[0].equals("AMOUNT") ? MetricGroup.AMOUNT : MetricGroup.DISTINCT;
            for (Metric metric : Metric.values()) {
                assertEquals(
                        group.metrics().contains(metric),
                        row.has(metric.columnName()),
                        "两个 Job 不能覆盖对方指标列");
            }
            String key = message[0] + "|" + dimensionKey(row) + "|" + row.path("shard_id").asInt();
            JsonNode previous = latest.get(key);
            if (previous == null
                    || row.path("update_seq").asLong() > previous.path("update_seq").asLong()) {
                latest.put(key, row);
            }
        }

        Map<String, long[]> rows() {
            Map<String, long[]> totals = new HashMap<>();
            for (JsonNode row : latest.values()) {
                long[] sum =
                        totals.computeIfAbsent(
                                dimensionKey(row), ignored -> new long[COLUMNS.length]);
                for (int index = 0; index < COLUMNS.length; index++) {
                    sum[index] += row.path(COLUMNS[index]).asLong();
                }
            }
            return totals;
        }

        boolean matches(Map<String, long[]> expected, boolean exactRows) {
            Map<String, long[]> actual = rows();
            if (exactRows && !expected.keySet().equals(actual.keySet())) {
                return false;
            }
            return expected.entrySet().stream()
                    .allMatch(entry -> Arrays.equals(entry.getValue(), actual.get(entry.getKey())));
        }

        private static String dimensionKey(JsonNode row) {
            return row.path("biz_date").asText()
                    + "|"
                    + row.path("dimension_type").asText()
                    + "|"
                    + row.path("dimension_id").asText();
        }
    }

    private static void addExpected(
            Map<String, long[]> expected, String date, long[] values, String... dimensions) {
        for (String dimension : dimensions) {
            expected.put(date + "|" + dimension, values);
        }
    }

    private static TradeEvent order(String id, String user, String shop, long time, int... skus) {
        TradeEvent event = new TradeEvent();
        event.eventId = "create-" + id;
        event.eventType = "ORDER_CREATED";
        event.businessId = id;
        event.orderId = id;
        event.checkoutId = "CK-" + id;
        event.userId = user;
        event.shopId = shop;
        boolean shanghai = shop.equals("S1");
        event.provinceId = shanghai ? "310000" : "330000";
        event.cityId = shanghai ? "310100" : "330100";
        event.districtId = shanghai ? "310115" : "330106";
        event.currency = "CNY";
        event.occurredAt = time;
        event.goodsCent = 3000L * skus.length;
        for (int index = 0; index < skus.length; index++) {
            TradeEvent.Line line = new TradeEvent.Line();
            line.orderLineId = id + "-L" + index;
            line.skuId = "SKU" + skus[index];
            String suffix = skus[index] <= 2 ? "1" : "2";
            line.productId = "P" + suffix;
            line.categoryId = "C" + suffix;
            line.brandId = "B" + suffix;
            line.amountCent = 3000;
            line.quantity = 1;
            event.lines.add(line);
        }
        return event;
    }

    private static TradeEvent paid(TradeEvent order) {
        TradeEvent paid = copy(order);
        paid.eventId = "pay-" + order.orderId;
        paid.eventType = "PAYMENT_SUCCEEDED";
        paid.businessId = "PAY-" + order.orderId;
        paid.occurredAt += 60000;
        paid.paidAt = paid.occurredAt;
        return paid;
    }

    private static TradeEvent refund(TradeEvent order, String id, long amount, long at) {
        TradeEvent refund = paid(order);
        refund.eventId = "refund-" + id;
        refund.eventType = "REFUND_SUCCEEDED";
        refund.businessId = id;
        refund.occurredAt = at;
        refund.goodsCent = amount;
        TradeEvent.Line line = refund.lines.get(0);
        line.amountCent = amount;
        line.quantity = 0;
        refund.lines = new ArrayList<>(List.of(line));
        return refund;
    }

    private static TradeEvent copy(TradeEvent event) {
        try {
            return Json.MAPPER.readValue(Json.write(event), TradeEvent.class);
        } catch (java.io.IOException exception) {
            throw new AssertionError(exception);
        }
    }

    private static List<String> envelopes(List<TradeEvent> events) {
        List<String> input = new ArrayList<>();
        for (TradeEvent event : events) {
            input.add(envelope(event, "c"));
        }
        return input;
    }

    private static String envelope(TradeEvent event, String operation) {
        var root = Json.MAPPER.createObjectNode().put("op", operation);
        root.putObject("source").put("table", "trade_outbox");
        root.putObject("after")
                .put("id", event.eventId)
                .put("order_id", event.orderId)
                .put("business_id", event.businessId)
                .put("event_type", event.eventType)
                .put("occurred_at", event.occurredAt)
                .put("payload_json", Json.write(event));
        return root.toString();
    }

    public static class HoldingSource implements SourceFunction<String> {
        private final List<String> input;
        private volatile boolean running = true;

        public HoldingSource(List<String> input) {
            this.input = input;
        }

        @Override
        public void run(SourceContext<String> context) throws Exception {
            for (String row : input) {
                synchronized (context.getCheckpointLock()) {
                    context.collect(row);
                }
            }
            synchronized (context.getCheckpointLock()) {
                context.emitWatermark(Watermark.MAX_WATERMARK);
            }
            synchronized (this) {
                while (running) {
                    wait();
                }
            }
        }

        @Override
        public synchronized void cancel() {
            running = false;
            notifyAll();
        }
    }

    /** 等所有分支处理完输入后再判定；保留源运行，让处理时间刷新 timer 能输出最终快照。 */
    public static class DrainSignalOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        @Override
        public void processElement(StreamRecord<String> element) {
            output.collect(element);
        }

        @Override
        public void processWatermark(Watermark watermark) throws Exception {
            if (watermark.getTimestamp() == Long.MAX_VALUE) {
                getProcessingTimeService()
                        .registerTimer(
                                getProcessingTimeService().getCurrentProcessingTime() + 250,
                                timestamp -> output.collect(new StreamRecord<>("DRAINED")));
            }
            super.processWatermark(watermark);
        }
    }
}
