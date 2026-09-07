package org.linyureal.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.linyureal.common.*;
import org.linyureal.config.AppConfig;
import org.linyureal.model.metric.*;
import org.linyureal.function.metric.*;
import org.linyureal.parser.FactParseFunction;
import org.linyureal.source.KafkaSources;
import org.linyureal.sink.*;
import java.util.Arrays;

public final class DimensionGmvJob {
    public static void main(String[] args) throws Exception {
        AppConfig c=AppConfig.load(args);
        StreamExecutionEnvironment env=JobRuntime.create(c,"metrics");
        SingleOutputStreamOperator<ItemFact> facts=env.fromSource(KafkaSources.create(c,
                Arrays.asList(c.topic("payment_fact"),c.topic("refund_fact")),c.get("group.metrics")),
                WatermarkStrategy.noWatermarks(),"money-facts").uid("lr-metric-source-v1")
                .process(new FactParseFunction()).uid("lr-fact-parser-v1");
        SingleOutputStreamOperator<ItemFact> accepted=facts.keyBy(f->f.fact_id)
                .process(new FactDeduplicator()).uid("lr-fact-dedup-v1");
        SingleOutputStreamOperator<MetricDelta> deltas=accepted.flatMap(
                (ItemFact f, org.apache.flink.util.Collector<MetricDelta> out)->{
                    for(MetricDelta d:DimensionExpander.expand(f)) out.collect(d);
                }).returns(MetricDelta.class).uid("lr-dimension-expand-v1");
        deltas.keyBy(MetricDelta::key).process(new DailyMetricFunction(c.positive("output.interval.ms")))
                .uid("lr-dimension-daily-v1")
                .sinkTo(DorisSinks.create(c,"doris.table.metrics","metrics")).uid("lr-metrics-doris-v1");
        accepted.map(Json::write).returns(String.class).uid("lr-detail-json-v1")
                .sinkTo(DorisSinks.create(c,"doris.table.facts","facts")).uid("lr-facts-doris-v1");
        facts.getSideOutput(Dirty.TAG).union(accepted.getSideOutput(Dirty.TAG))
                .sinkTo(KafkaSinks.create(c,c.topic("dirty"),"metric-dirty")).uid("lr-metric-dirty-v1");
        env.execute("LinyuReal Dimension GMV");
    }
}
