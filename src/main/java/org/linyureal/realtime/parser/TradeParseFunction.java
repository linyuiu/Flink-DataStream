package org.linyureal.realtime.parser;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.RowChange;
import static org.linyureal.realtime.function.TradeProcessFunction.DIRTY;

public class TradeParseFunction extends ProcessFunction<String, RowChange> {
    @Override public void processElement(String value, Context ctx, Collector<RowChange> out) {
        if (value == null || value.equals("null")) return;
        RowChange c;
        try { c = CdcRows.trade(value); }
        catch (RuntimeException e) { ctx.output(DIRTY, Jsons.dirty("trade-parse", value, e.getMessage()).toString()); return; }
        out.collect(c);
    }
}
