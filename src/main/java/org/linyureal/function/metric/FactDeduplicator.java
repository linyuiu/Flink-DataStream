package org.linyureal.function.metric;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.*;
import org.linyureal.model.metric.*;

public class FactDeduplicator extends KeyedProcessFunction<String,ItemFact,ItemFact> {
    private transient ValueState<String> seen;
    @Override public void open(OpenContext ctx) {
        seen=getRuntimeContext().getState(new ValueStateDescriptor<>("immutable-fact-v1",String.class));
    }
    @Override public void processElement(ItemFact fact,Context ctx,Collector<ItemFact> out) throws Exception {
        String content=Json.write(fact), old=seen.value();
        if(old!=null) {
            if(!old.equals(content)) ctx.output(Dirty.TAG,Dirty.record("fact-conflict",content,"Same fact ID changed"));
            return;
        }
        try { DimensionExpander.expand(fact); }
        catch(IllegalArgumentException e) { ctx.output(Dirty.TAG,Dirty.record("fact-validation",content,e.getMessage())); return; }
        seen.update(content);
        out.collect(fact);
    }
}
