package org.linyureal.parser;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.*;
import org.linyureal.function.metric.DimensionExpander;
import org.linyureal.model.metric.ItemFact;

public class FactParseFunction extends ProcessFunction<String,ItemFact> {
    @Override public void processElement(String raw,Context ctx,Collector<ItemFact> out) throws Exception {
        ItemFact fact;
        try { fact=Json.MAPPER.readValue(raw,ItemFact.class); DimensionExpander.expand(fact); }
        catch(Exception e) { ctx.output(Dirty.TAG,Dirty.record("fact-parse",raw,e.getMessage())); return; }
        out.collect(fact);
    }
}
