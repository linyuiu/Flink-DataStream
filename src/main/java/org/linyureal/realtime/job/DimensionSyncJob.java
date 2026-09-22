package org.linyureal.realtime.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.linyureal.realtime.common.*;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.parser.DimensionCodec;
import org.linyureal.realtime.parser.DimensionParseFunction;
import org.linyureal.realtime.function.LatestDimensionFunction;
import org.linyureal.realtime.source.KafkaIo;
import org.linyureal.realtime.sink.DorisSinkFactory;
import java.util.stream.Collectors;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public final class DimensionSyncJob {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args); StreamExecutionEnvironment env = Environments.create(c, "dimensions");
        SingleOutputStreamOperator<String> parsed = env.fromSource(KafkaIo.source(c,
                Contracts.DIM_TABLES.stream().map(c::topic).collect(Collectors.toList()), "group.dimensions"),
                WatermarkStrategy.noWatermarks(), "ods-dimensions").uid("rttrade-dim-source-v1")
                .process(new DimensionParseFunction()).uid("rttrade-dim-parser-v1");
        SingleOutputStreamOperator<String> accepted = parsed.keyBy(DimensionCodec::key)
                .process(new LatestDimensionFunction()).uid("rttrade-dim-latest-v1");
        accepted.sinkTo(DorisSinkFactory.create(c, "doris.table.dimensions", "dimensions")).uid("rttrade-dim-doris-v1");
        parsed.getSideOutput(DIRTY).union(accepted.getSideOutput(DIRTY))
                .sinkTo(KafkaIo.sink(c, c.topic("dirty"), "dim-dirty")).uid("rttrade-dim-dirty-v1");
        env.execute("RTTrade Dimension Catalog");
    }
}
