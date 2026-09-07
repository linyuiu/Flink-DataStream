package org.linyureal.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.linyureal.common.*;
import org.linyureal.config.AppConfig;
import org.linyureal.model.cdc.TradeEnvelope;
import org.linyureal.model.metric.ItemFact;
import org.linyureal.function.trade.TradeFactFunction;
import org.linyureal.parser.ChangeParseFunction;
import org.linyureal.source.KafkaSources;
import org.linyureal.sink.KafkaSinks;
import org.linyureal.validation.TradeValidator;
import java.util.stream.Collectors;

public final class TradeFactJob {
    public static void main(String[] args) throws Exception {
        AppConfig c=AppConfig.load(args);
        StreamExecutionEnvironment env=JobRuntime.create(c,"trade");
        SingleOutputStreamOperator<TradeEnvelope> changes=env.fromSource(
                KafkaSources.create(c,TradeValidator.TABLES.stream().map(c::topic).collect(Collectors.toList()),c.get("group.trade")),
                WatermarkStrategy.noWatermarks(),"trade-cdc").uid("lr-trade-source-v1")
                .process(new ChangeParseFunction()).uid("lr-trade-parser-v1");
        SingleOutputStreamOperator<ItemFact> facts=changes.keyBy(TradeEnvelope::orderId)
                .process(new TradeFactFunction(c.positive("join.audit.interval.ms"))).uid("lr-trade-facts-v1");
        facts.filter(f->"PAYMENT".equals(f.fact_type)).uid("lr-payment-filter-v1")
                .map(Json::write).returns(String.class).uid("lr-payment-json-v1")
                .sinkTo(KafkaSinks.create(c,c.topic("payment_fact"),"payments")).uid("lr-payment-kafka-v1");
        facts.filter(f->"REFUND".equals(f.fact_type)).uid("lr-refund-filter-v1")
                .map(Json::write).returns(String.class).uid("lr-refund-json-v1")
                .sinkTo(KafkaSinks.create(c,c.topic("refund_fact"),"refunds")).uid("lr-refund-kafka-v1");
        changes.getSideOutput(Dirty.TAG).union(facts.getSideOutput(Dirty.TAG))
                .sinkTo(KafkaSinks.create(c,c.topic("dirty"),"trade-dirty")).uid("lr-trade-dirty-v1");
        env.execute("LinyuReal Trade Facts");
    }
}
