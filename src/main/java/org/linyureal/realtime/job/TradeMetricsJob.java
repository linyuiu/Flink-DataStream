package org.linyureal.realtime.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.function.*;
import org.linyureal.realtime.model.*;
import org.linyureal.realtime.parser.FactParseFunction;
import org.linyureal.realtime.source.KafkaIo;
import org.linyureal.realtime.sink.DorisSinkFactory;
import java.util.List;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public final class TradeMetricsJob {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args); StreamExecutionEnvironment env = Environments.create(c, "metrics");
        SingleOutputStreamOperator<TradeFact> parsed = env.fromSource(KafkaIo.source(c, List.of(c.topic("facts")), "group.metrics"),
                WatermarkStrategy.noWatermarks(), "dwd-facts").uid("rttrade-dwd-source-v1")
                .process(new FactParseFunction()).uid("rttrade-fact-parse-v1");
        SingleOutputStreamOperator<TradeFact> accepted = parsed.keyBy(f -> f.fact_id)
                .process(new FactDedupFunction()).uid("rttrade-fact-dedup-v1");
        accepted.flatMap((TradeFact f, org.apache.flink.util.Collector<MetricInput> out) -> {
            for (MetricInput input : MetricLogic.expand(f)) out.collect(input);
        }).returns(MetricInput.class).uid("rttrade-dim-expand-v1").keyBy(MetricInput::key)
                .process(new DailyMetricsFunction(c.positive("output.interval.ms"))).uid("rttrade-daily-metrics-v1")
                .sinkTo(DorisSinkFactory.create(c, "doris.table.metrics", "metrics")).uid("rttrade-metrics-doris-v1");
        accepted.map(Jsons::write).returns(String.class).uid("rttrade-fact-json-v1")
                .sinkTo(DorisSinkFactory.create(c, "doris.table.facts", "facts")).uid("rttrade-fact-doris-v1");
        parsed.getSideOutput(DIRTY).union(accepted.getSideOutput(DIRTY))
                .sinkTo(KafkaIo.sink(c, c.topic("dirty"), "metrics-dirty")).uid("rttrade-metric-dirty-v1");
        env.execute("RTTrade Multi-dimension Metrics");
    }
}
