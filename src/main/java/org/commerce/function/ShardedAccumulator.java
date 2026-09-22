package org.commerce.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.commerce.common.Json;
import org.commerce.common.Metric;
import org.commerce.common.MetricGroup;
import org.commerce.model.MetricDelta;

import java.util.List;

/** 每个“日期 + 维度 + 分片”维护绝对累计值，定时写 Doris。 全站汇总交给 Doris SUM 分片，避免在 Flink 内把所有金额再次汇聚到一个热 key。 */
public class ShardedAccumulator extends KeyedProcessFunction<String, MetricDelta, String> {
    private final long metricDays;
    private final long flushMillis;
    private final List<Metric> activeMetrics;
    private final String statePrefix;
    private transient ValueState<long[]> totals;
    private transient ValueState<Long> nextFlushAt;
    private transient ValueState<Long> expiresAt;
    private transient ValueState<Long> sequence;

    public ShardedAccumulator(long metricDays, long flushMillis) {
        this.metricDays = metricDays;
        this.flushMillis = flushMillis;
        this.activeMetrics = List.of(Metric.values());
        this.statePrefix = "";
    }

    /** 新任务只保存其负责的指标：金额组 9 个 long，去重计数组 2 个 long。 */
    public ShardedAccumulator(long metricDays, long flushMillis, MetricGroup group) {
        this.metricDays = metricDays;
        this.flushMillis = flushMillis;
        this.activeMetrics = group.metrics();
        this.statePrefix = group.statePrefix() + "-";
    }

    @Override
    public void open(OpenContext context) {
        // descriptor 名称与类型属于持久状态契约，整理 Java 变量名时不能一起更改。
        totals =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        statePrefix + "partial-totals-v1", long[].class));
        nextFlushAt =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>(statePrefix + "flush-v1", Long.class));
        expiresAt =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(statePrefix + "expiry-v1", Long.class));
        sequence =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        statePrefix + "sequence-v1", Long.class));
    }

    @Override
    public void processElement(MetricDelta delta, Context context, Collector<String> output)
            throws Exception {
        long now = context.timerService().currentProcessingTime();
        if (!Horizons.open(delta.date, now, metricDays)) {
            // 历史日已经关账，不能把状态从 0 重新累加并覆盖 Doris 中的完整历史值。
            context.output(
                    Outputs.REPAIR,
                    Outputs.record(
                            "metric-horizon",
                            Json.write(delta),
                            "Target business date closed; this leg requires backfill"));
            return;
        }

        accumulate(delta);
        registerTimers(delta.date, now, context);
    }

    private void accumulate(MetricDelta delta) throws Exception {
        long[] previous = totals.value();
        long[] updated = previous == null ? new long[activeMetrics.size()] : previous.clone();
        for (int index = 0; index < updated.length; index++) {
            updated[index] = Math.addExact(updated[index], delta.value(activeMetrics.get(index)));
        }
        totals.update(updated);
    }

    private void registerTimers(String date, long now, Context context) throws Exception {
        if (expiresAt.value() == null) {
            long closeTime = Horizons.expires(date, metricDays);
            expiresAt.update(closeTime);
            context.timerService().registerProcessingTimeTimer(closeTime);
        }
        if (nextFlushAt.value() == null) {
            // 同一刷新周期内继续合并增量，只保留一个待触发的刷新 timer。
            long flushTime = Math.min(now + flushMillis, expiresAt.value());
            nextFlushAt.update(flushTime);
            context.timerService().registerProcessingTimeTimer(flushTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<String> output)
            throws Exception {
        if (nextFlushAt.value() != null && timestamp == nextFlushAt.value()) {
            emitSnapshot(context.getCurrentKey(), timestamp, output);
            nextFlushAt.clear();
        }
        if (expiresAt.value() != null && timestamp == expiresAt.value()) {
            // 刷新和到期可能是同一个时刻，必须先输出最后的小计，再清理状态。
            totals.clear();
            nextFlushAt.clear();
            expiresAt.clear();
            sequence.clear();
        }
    }

    private void emitSnapshot(String partitionKey, long timestamp, Collector<String> output)
            throws Exception {
        String[] keyParts = partitionKey.split("\\|", -1);
        long[] currentTotals = totals.value();
        long nextSequence = sequence.value() == null ? 1 : Math.addExact(sequence.value(), 1);
        sequence.update(nextSequence);

        var row =
                Json.MAPPER
                        .createObjectNode()
                        .put("biz_date", keyParts[0])
                        .put("dimension_type", keyParts[1])
                        .put("dimension_id", keyParts[2])
                        .put("shard_id", Integer.parseInt(keyParts[3]))
                        .put("update_seq", nextSequence)
                        .put("updated_at_ms", timestamp);
        for (int index = 0; index < currentTotals.length; index++) {
            row.put(activeMetrics.get(index).columnName(), currentTotals[index]);
        }
        output.collect(row.toString());
    }
}
