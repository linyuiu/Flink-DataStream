package org.linyureal.realtime;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.*;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.function.*;
import org.linyureal.realtime.mock.*;
import org.linyureal.realtime.model.*;
import org.linyureal.realtime.parser.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Local Flink execution, no external MySQL/Kafka/Doris connection. */
class RuntimePipelineTest {
    @Test @Timeout(45)
    void cdcParserProjectionAndNetworkSerializationExecute() throws Exception {
        List<String> values = new ArrayList<>();
        for (RowChange change : TradeDesignTest.rows(Scenario.FULL_GOODS_REFUND, "MINI")) {
            var n = Jsons.MAPPER.createObjectNode(); n.putObject("source").put("table", change.table);
            n.put("op", change.operation).set("after", Jsons.object(change.rowJson)); values.add(n.toString());
        }
        values.addAll(new ArrayList<>(values)); Collections.shuffle(values, new Random(42));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); env.setParallelism(2);
        List<String> output = new ArrayList<>();
        try (CloseableIterator<String> it = env.fromCollection(values).process(new TradeParseFunction()).keyBy(r -> r.orderId)
                .process(new TradeProcessFunction(300000)).executeAndCollect()) {
            while (it.hasNext()) output.add(it.next());
        }
        assertEquals(7, output.size());
        long paid = 0, refunded = 0;
        for (String s : output) {
            TradeFact f = MetricLogic.parse(s);
            if (f.fact_type.equals("PAID")) paid += f.amount_cent;
            if (f.fact_type.equals("REFUNDED")) refunded += f.amount_cent;
        }
        assertTrue(paid > 0); assertEquals(paid, refunded);
    }
    @Test @Timeout(45)
    void keyedDistinctAndProcessingTimerFlushFinalAmounts() throws Exception {
        List<TradeFact> expected = TradeDesignTest.run(TradeDesignTest.rows(Scenario.MULTIPLE_REFUNDS, "METRIC"));
        List<String> json = new ArrayList<>(); expected.forEach(f -> json.add(Jsons.write(f))); json.addAll(new ArrayList<>(json));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); env.setParallelism(1);
        var parsed = env.addSource(new HoldingSource(json)).process(new FactParseFunction());
        var result = parsed.keyBy(f -> f.fact_id).process(new FactDedupFunction())
                .flatMap((TradeFact f, org.apache.flink.util.Collector<MetricInput> out) -> {
                    for (MetricInput in : MetricLogic.expand(f)) if (in.dimensionType.equals("ALL")) out.collect(in);
                }).returns(MetricInput.class).keyBy(MetricInput::key).process(new DailyMetricsFunction(100));
        long refund = TradeDesignTest.amount(expected, "REFUNDED");
        try (CloseableIterator<String> it = result.executeAndCollect()) {
            while (it.hasNext()) {
                var row = Jsons.object(it.next());
                if (row.path("refund_amount_cent").asLong() != refund) continue;
                assertEquals(1, row.path("paid_order_count").asLong()); assertEquals(1, row.path("paid_user_count").asLong());
                assertEquals(2, row.path("refund_request_count").asLong());
                assertEquals(TradeDesignTest.amount(expected, "PAID") - refund, row.path("net_paid_amount_cent").asLong());
                return;
            }
        }
        fail("No final metric output");
    }
    @Test @Timeout(45)
    void latestDimensionNameWinsDespiteOldUpdates() throws Exception {
        var original = Catalog.rows().get("dim_shop").get(0); List<String> input = new ArrayList<>();
        for (var row : List.of(original, original.deepCopy().put("version", 2).put("name", "New name"), original)) {
            var envelope = Jsons.MAPPER.createObjectNode(); envelope.putObject("source").put("table", "dim_shop");
            envelope.put("op", "u").set("after", row); input.add(envelope.toString());
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); env.setParallelism(1);
        List<String> result = new ArrayList<>();
        try (CloseableIterator<String> it = env.fromCollection(input).process(new DimensionParseFunction())
                .keyBy(DimensionCodec::key).process(new LatestDimensionFunction()).executeAndCollect()) {
            while (it.hasNext()) result.add(it.next());
        }
        assertEquals(2, result.size()); assertEquals("New name", Jsons.object(result.get(1)).path("name").asText());
    }
    public static class HoldingSource implements SourceFunction<String> {
        private final List<String> values;
        private volatile boolean running = true;
        public HoldingSource(List<String> values) { this.values = values; }
        @Override public void run(SourceContext<String> ctx) throws Exception {
            for (String value : values) synchronized (ctx.getCheckpointLock()) { ctx.collect(value); }
            synchronized (this) { while (running) wait(); }
        }
        @Override public synchronized void cancel() { running = false; notifyAll(); }
    }
}
