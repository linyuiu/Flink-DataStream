package org.commerce.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.commerce.common.Json;
import org.commerce.common.Metric;
import org.commerce.model.MetricDelta;

/** 按“日 + 维度 + 用户/订单”保存是否见过，避免在全站 key 下维护一个不断增长的大集合。 */
public class ExactDistinctFunction extends KeyedProcessFunction<String, MetricDelta, MetricDelta> {
    private final long eventDays;
    private transient ValueState<Boolean> seen;

    public ExactDistinctFunction(long eventDays) {
        this.eventDays = eventDays;
    }

    @Override
    public void open(OpenContext context) {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("distinct-v1", Boolean.class);
        descriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(java.time.Duration.ofDays(eventDays + 2))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .cleanupInRocksdbCompactFilter(1000)
                        .build());
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(MetricDelta delta, Context context, Collector<MetricDelta> output)
            throws Exception {
        if (!Horizons.open(delta.date, context.timerService().currentProcessingTime(), eventDays)) {
            context.output(
                    Outputs.REPAIR,
                    Outputs.record(
                            "distinct-horizon",
                            Json.write(delta),
                            "Expired event-day distinct bucket"));
            return;
        }
        if (Boolean.TRUE.equals(seen.value())) {
            return;
        }
        seen.update(true);
        if (delta.distinctKind.equals("PAID_USER")) {
            delta.set(Metric.PAID_USERS, 1);
        } else if (delta.distinctKind.equals("REFUND_ORDER")) {
            delta.set(Metric.REFUND_ORDERS, 1);
        } else {
            throw new IllegalArgumentException("Unknown distinct kind");
        }
        output.collect(delta);
    }
}
