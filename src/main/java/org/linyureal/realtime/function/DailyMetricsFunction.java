package org.linyureal.realtime.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.*;
import org.linyureal.realtime.model.MetricInput;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;

public class DailyMetricsFunction extends KeyedProcessFunction<String, MetricInput, String> {
    private transient ValueState<long[]> totals;
    private transient ValueState<Long> timer;
    private transient MapState<String, Boolean> distinct;
    private final long interval;
    public DailyMetricsFunction(long interval) { this.interval = interval; }
    @Override public void open(OpenContext ctx) {
        totals = getRuntimeContext().getState(new ValueStateDescriptor<>("totals-v1", long[].class));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("flush-v1", Long.class));
        distinct = getRuntimeContext().getMapState(new MapStateDescriptor<>("distinct-v1", String.class, Boolean.class));
    }
    @Override public void processElement(MetricInput in, Context ctx, Collector<String> out) throws Exception {
        long[] previous = totals.value();
        totals.update(MetricLogic.add(previous == null ? new long[Contracts.METRICS.length] : previous, in, key -> {
            try {
                if (distinct.contains(key)) return false;
                distinct.put(key, true); return true;
            } catch (Exception e) { throw new IllegalStateException("State backend failed", e); }
        }));
        if (timer.value() == null) {
            long next = ctx.timerService().currentProcessingTime() + interval;
            timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
        }
    }
    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String[] key = ctx.getCurrentKey().split("\\|", -1); long[] t = totals.value();
        ObjectNode n = Jsons.MAPPER.createObjectNode();
        n.put("biz_date", key[0]); n.put("dimension_type", key[1]); n.put("dimension_id", key[2]); n.put("currency", "CNY");
        for (int i = 0; i < t.length; i++) n.put(Contracts.METRICS[i], t[i]);
        n.put("updated_at", java.time.LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                java.time.ZoneId.of("Asia/Shanghai")).format(Jsons.TIME));
        out.collect(n.toString()); timer.clear();
    }
}
