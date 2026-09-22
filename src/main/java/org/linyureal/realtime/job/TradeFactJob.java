package org.linyureal.realtime.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.linyureal.realtime.common.Contracts;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.function.TradeProcessFunction;
import org.linyureal.realtime.model.RowChange;
import org.linyureal.realtime.parser.TradeParseFunction;
import org.linyureal.realtime.source.KafkaIo;
import java.util.stream.Collectors;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public final class TradeFactJob {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args); StreamExecutionEnvironment env = Environments.create(c, "facts");
        SingleOutputStreamOperator<RowChange> rows = env.fromSource(KafkaIo.source(c,
                Contracts.TRADE_TABLES.stream().map(c::topic).collect(Collectors.toList()), "group.facts"),
                WatermarkStrategy.noWatermarks(), "ods-trade").uid("rttrade-ods-source-v1")
                .process(new TradeParseFunction()).uid("rttrade-row-parser-v1");
        SingleOutputStreamOperator<String> facts = rows.keyBy(r -> r.orderId)
                .process(new TradeProcessFunction(c.positive("join.audit.ms"))).uid("rttrade-projector-v1");
        facts.sinkTo(KafkaIo.sink(c, c.topic("facts"), "facts")).uid("rttrade-facts-kafka-v1");
        rows.getSideOutput(DIRTY).union(facts.getSideOutput(DIRTY))
                .sinkTo(KafkaIo.sink(c, c.topic("dirty"), "facts-dirty")).uid("rttrade-facts-dirty-v1");
        env.execute("RTTrade Order Lifecycle Facts");
    }
}
