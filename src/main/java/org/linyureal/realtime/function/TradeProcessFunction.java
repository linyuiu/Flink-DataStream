package org.linyureal.realtime.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.*;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.*;
import java.util.List;

public class TradeProcessFunction extends KeyedProcessFunction<String, RowChange, String> {
    public static final OutputTag<String> DIRTY = new OutputTag<String>("rttrade-dirty-v1") {};
    private transient ValueState<OrderState> state;
    private transient ValueState<Long> audit;
    private final long auditMillis;
    public TradeProcessFunction(long auditMillis) { this.auditMillis = auditMillis; }
    @Override public void open(OpenContext context) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("trade-projection-v1", OrderState.class));
        audit = getRuntimeContext().getState(new ValueStateDescriptor<>("audit-v1", Long.class));
    }
    @Override public void processElement(RowChange c, Context ctx, Collector<String> out) throws Exception {
        OrderState old = state.value(), next = old == null ? new OrderState() : new OrderState(old);
        List<TradeFact> facts;
        try { facts = TradeProjector.accept(next, c); }
        catch (IllegalArgumentException | ArithmeticException e) {
            ctx.output(DIRTY, Jsons.dirty("trade-projection", Jsons.write(c), e.getMessage()).toString()); return;
        }
        state.update(next);
        for (TradeFact f : facts) out.collect(Jsons.write(f));
        if (audit.value() == null) {
            long time = ctx.timerService().currentProcessingTime() + auditMillis;
            audit.update(time); ctx.timerService().registerProcessingTimeTimer(time);
        }
    }
    @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        OrderState s = state.value();
        if (s != null) {
            String reason = TradeProjector.incompleteReason(s);
            if (reason != null)
                ctx.output(DIRTY, Jsons.dirty("join-timeout", ctx.getCurrentKey(),
                        reason + "; state retained, reconcile source transaction").toString());
        }
        // One audit per activity period, not an endless alert loop. No monetary state TTL.
        audit.clear();
    }
}
