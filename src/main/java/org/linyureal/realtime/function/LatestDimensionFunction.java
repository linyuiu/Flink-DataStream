package org.linyureal.realtime.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.Jsons;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public class LatestDimensionFunction extends KeyedProcessFunction<String, String, String> {
    private transient ValueState<String> state;
    @Override public void open(OpenContext c) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("dim-v1", String.class));
    }
    @Override public void processElement(String json, Context ctx, Collector<String> out) throws Exception {
        String old = state.value(); long version = Jsons.integer(Jsons.object(json), "version");
        if (old != null) {
            long oldVersion = Jsons.integer(Jsons.object(old), "version");
            if (version < oldVersion) return;
            if (version == oldVersion) {
                if (!Jsons.object(old).equals(Jsons.object(json)))
                    ctx.output(DIRTY, Jsons.dirty("dimension-conflict", json, "Same version changed").toString());
                return;
            }
        }
        state.update(json); out.collect(json);
    }
}
