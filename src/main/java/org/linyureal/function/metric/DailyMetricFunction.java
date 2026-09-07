package org.linyureal.function.metric;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.Json;
import org.linyureal.model.metric.MetricDelta;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;

public class DailyMetricFunction extends KeyedProcessFunction<String,MetricDelta,String> {
    private final long interval;
    private transient ValueState<long[]> totals;
    private transient ValueState<Long> timer;
    public DailyMetricFunction(long interval) { this.interval=interval; }
    @Override public void open(OpenContext ctx) {
        totals=getRuntimeContext().getState(new ValueStateDescriptor<>("dimension-totals-v1",long[].class));
        timer=getRuntimeContext().getState(new ValueStateDescriptor<>("output-timer-v1",Long.class));
    }
    @Override public void processElement(MetricDelta d,Context ctx,Collector<String> out) throws Exception {
        long[] old=totals.value(); if(old==null) old=new long[3];
        totals.update(new long[]{Math.addExact(old[0],d.paid),Math.addExact(old[1],d.refunded),Math.addExact(old[2],d.net)});
        if(timer.value()==null) {
            long now=ctx.timerService().currentProcessingTime(), next=now-now%interval+interval;
            timer.update(next); ctx.timerService().registerProcessingTimeTimer(next);
        }
    }
    @Override public void onTimer(long timestamp,OnTimerContext ctx,Collector<String> out) throws Exception {
        String[] key=ctx.getCurrentKey().split("\\|",-1);
        long[] t=totals.value();
        ObjectNode n=Json.MAPPER.createObjectNode();
        n.put("biz_date",key[0]); n.put("dimension_type",key[1]); n.put("dimension_id",key[2]); n.put("currency_code",key[3]);
        n.put("paid_gmv",BigDecimal.valueOf(t[0],2));
        n.put("refund_amount",BigDecimal.valueOf(t[1],2));
        n.put("net_paid_gmv",BigDecimal.valueOf(t[2],2));
        n.put("update_time",java.time.LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(timestamp),
                java.time.ZoneId.of("Asia/Shanghai")).format(org.linyureal.validation.TradeValidator.TIME));
        out.collect(n.toString()); timer.clear();
    }
}
