package org.commerce.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.commerce.config.CommerceConfig;
import org.commerce.model.MetricDelta;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

/** 将一笔交易事实展开为日维度增量；分片参数在构建拓扑时读取。 */
public final class ExpandTradeMetrics implements FlatMapFunction<String, MetricDelta> {
    private final int globalShards;
    private final int dimensionShards;
    private final int entityShards;

    public ExpandTradeMetrics(CommerceConfig config) {
        globalShards = Math.toIntExact(config.positive("shards.global"));
        dimensionShards = Math.toIntExact(config.positive("shards.dimensions"));
        entityShards = Math.toIntExact(config.positive("shards.entities"));
    }

    @Override
    public void flatMap(String raw, Collector<MetricDelta> output) {
        TradeEvent event = EventContract.parse(raw);
        for (MetricDelta delta :
                EventExpansion.expand(event, globalShards, dimensionShards, entityShards)) {
            output.collect(delta);
        }
    }
}
