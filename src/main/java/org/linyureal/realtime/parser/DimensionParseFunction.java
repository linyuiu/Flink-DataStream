package org.linyureal.realtime.parser;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.Jsons;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public class DimensionParseFunction extends ProcessFunction<String, String> {
    @Override public void processElement(String raw, Context ctx, Collector<String> out) {
        String result;
        try { result = DimensionCodec.parse(raw); }
        catch (RuntimeException e) {
            ctx.output(DIRTY, Jsons.dirty("dimension-parse", raw, e.getMessage()).toString()); return;
        }
        out.collect(result);
    }
}
