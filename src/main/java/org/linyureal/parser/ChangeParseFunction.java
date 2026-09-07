package org.linyureal.parser;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.Dirty;
import org.linyureal.model.cdc.Change;
import org.linyureal.model.cdc.TradeEnvelope;

public class ChangeParseFunction extends ProcessFunction<String,TradeEnvelope> {
    @Override public void processElement(String raw,Context ctx,Collector<TradeEnvelope> out) throws Exception {
        Change c;
        try { c=ChangeParser.parse(raw); }
        catch(Exception e) { ctx.output(Dirty.TAG,Dirty.record("parse",raw,e.getMessage())); return; }
        // Keep downstream exceptions out of the parse-error handler.
        if(c!=null) out.collect(new TradeEnvelope(c));
    }
}
