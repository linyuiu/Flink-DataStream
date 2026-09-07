package org.linyureal.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.linyureal.common.Dirty;
import org.linyureal.config.AppConfig;
import org.linyureal.model.dimension.DimensionRow;
import org.linyureal.parser.DimensionParseFunction;
import org.linyureal.function.metric.DimensionVersionFunction;
import org.linyureal.source.KafkaSources;
import org.linyureal.sink.*;
import java.util.stream.Collectors;

public final class DimensionSyncJob {
    public static void main(String[] args) throws Exception {
        AppConfig c=AppConfig.load(args);
        StreamExecutionEnvironment env=JobRuntime.create(c,"dimensions");
        SingleOutputStreamOperator<DimensionRow> rows=env.fromSource(KafkaSources.create(c,
                DimensionParseFunction.TABLES.stream().map(c::topic).collect(Collectors.toList()),c.get("group.dimensions")),
                WatermarkStrategy.noWatermarks(),"dimension-cdc").uid("lr-dim-source-v1")
                .process(new DimensionParseFunction()).uid("lr-dim-parser-v1");
        SingleOutputStreamOperator<String> updates=rows.keyBy(DimensionRow::key)
                .process(new DimensionVersionFunction()).uid("lr-dim-version-v1");
        updates.sinkTo(DorisSinks.create(c,"doris.table.dimensions","dimensions")).uid("lr-dim-doris-v1");
        rows.getSideOutput(Dirty.TAG).union(updates.getSideOutput(Dirty.TAG))
                .sinkTo(KafkaSinks.create(c,c.topic("dirty"),"dimension-dirty")).uid("lr-dim-dirty-v1");
        env.execute("LinyuReal Dimension Sync");
    }
}
