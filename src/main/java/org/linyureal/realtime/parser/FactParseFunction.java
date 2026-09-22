package org.linyureal.realtime.parser;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.function.MetricLogic;
import org.linyureal.realtime.model.TradeFact;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public class FactParseFunction extends ProcessFunction<String, TradeFact> {
    @Override public void processElement(String raw, Context ctx, Collector<TradeFact> out) {
        TradeFact f;
        try { f = MetricLogic.parse(raw); }
        catch (RuntimeException e) { ctx.output(DIRTY, Jsons.dirty("fact-parse", raw, e.getMessage()).toString()); return; }
        out.collect(f);
    }
}
