package org.linyureal.function.trade;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.model.cdc.Change;
import org.linyureal.model.cdc.TradeEnvelope;
import org.linyureal.model.metric.ItemFact;
import org.linyureal.model.trade.TradeState;
import org.linyureal.common.Json;
import org.linyureal.common.Dirty;
import java.util.List;

public class TradeFactFunction extends KeyedProcessFunction<String,TradeEnvelope,ItemFact> {
    private transient ValueState<TradeState> state;
    private transient ValueState<Long> auditTimer;
    private final long auditInterval;
    public TradeFactFunction(long auditInterval) { this.auditInterval=auditInterval; }
    @Override public void open(OpenContext ctx) {
        state=getRuntimeContext().getState(new ValueStateDescriptor<>("trade-rows-v1",TradeState.class));
        auditTimer=getRuntimeContext().getState(new ValueStateDescriptor<>("audit-timer-v1",Long.class));
    }
    @Override public void processElement(TradeEnvelope envelope,Context ctx,Collector<ItemFact> out) throws Exception {
        Change c=envelope.decode();
        TradeState old=state.value();
        TradeState next=old==null ? new TradeState() : new TradeState(old);
        List<ItemFact> facts;
        try { facts=TradeAssembler.accept(next,c); }
        catch(IllegalArgumentException | ArithmeticException e) {
            ctx.output(Dirty.TAG,Dirty.record("trade",Json.write(c),e.getMessage())); return;
        }
        state.update(next);
        for(ItemFact fact:facts) out.collect(fact);
        if(auditTimer.value()==null) {
            long t=ctx.timerService().currentProcessingTime()+auditInterval;
            auditTimer.update(t); ctx.timerService().registerProcessingTimeTimer(t);
        }
    }
    @Override public void onTimer(long timestamp,OnTimerContext ctx,Collector<ItemFact> out) throws Exception {
        TradeState s=state.value();
        if(s!=null) {
            long expected=0;
            for(String k:s.rows.keySet()) {
                if(k.startsWith("trade_payment_item:")) expected++;
                if(k.startsWith("trade_refund:")) {
                    com.fasterxml.jackson.databind.JsonNode header=Json.MAPPER.readTree(s.rows.get(k));
                    if("SUCCEEDED".equals(header.path("status").asText())) expected+=header.path("item_count").asLong();
                }
                // Refund detail may be pending/failed; audit only success headers.
                if(k.startsWith("trade_refund_item:")) {
                    com.fasterxml.jackson.databind.JsonNode r=Json.MAPPER.readTree(s.rows.get(k));
                    String header=s.rows.get("trade_refund:"+r.path("refund_id").asText());
                    if(header==null) expected++;
                }
            }
            boolean paymentWithoutFact=s.rows.keySet().stream().anyMatch(k->k.startsWith("trade_payment:")) && s.emitted.isEmpty();
            for(String k:s.rows.keySet()) if(k.startsWith("trade_order:") && s.emitted.isEmpty()) {
                String status=Json.MAPPER.readTree(s.rows.get(k)).path("status").asText();
                if("PAID".equals(status) || "COMPLETED".equals(status)) paymentWithoutFact=true;
            }
            if(expected>s.emitted.size() || paymentWithoutFact)
                ctx.output(Dirty.TAG,Dirty.record("join-timeout",ctx.getCurrentKey(),"Incomplete successful trade; retained for late arrival, reconcile source tables"));
        }
        auditTimer.clear();
    }
}
