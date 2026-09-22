package org.linyureal.realtime.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.TradeFact;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public class FactDedupFunction extends KeyedProcessFunction<String, TradeFact, TradeFact> {
    private transient ValueState<String> seen;
    @Override public void open(OpenContext c) { seen = getRuntimeContext().getState(new ValueStateDescriptor<>("fact-v1", String.class)); }
    @Override public void processElement(TradeFact f, Context ctx, Collector<TradeFact> out) throws Exception {
        String json = Jsons.write(f), old = seen.value();
        if (old != null) {
            if (!old.equals(json)) ctx.output(DIRTY, Jsons.dirty("fact-conflict", json, "Same fact ID changed").toString());
            return;
        }
        seen.update(json); out.collect(f);
    }
}
