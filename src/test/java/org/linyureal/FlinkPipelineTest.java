package org.linyureal;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.linyureal.common.Json;
import org.linyureal.function.trade.TradeFactFunction;
import org.linyureal.function.metric.FactDeduplicator;
import org.linyureal.mock.generator.TradeGenerator;
import org.linyureal.mock.scenario.Scenario;
import org.linyureal.model.metric.ItemFact;
import org.linyureal.parser.ChangeParseFunction;
import java.time.LocalDateTime;
import java.util.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Local Flink runtime smoke test, no Kafka/MySQL/Doris connection. */
class FlinkPipelineTest {
    @Test void sourceParserKeyedJoinAndFactDedupRunInFlink119() throws Exception {
        List<String> input=new ArrayList<>();
        TradeGenerator.generate("FLINK",Scenario.FULL_REFUND,LocalDateTime.of(2026,8,11,10,0),new Random(1))
                .forEach(tx->tx.forEach(c->input.add(Json.write(c))));
        input.addAll(new ArrayList<>(input)); Collections.shuffle(input,new Random(7));
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        long paid=0,refunded=0; int count=0;
        try(CloseableIterator<ItemFact> it=env.fromCollection(input).process(new ChangeParseFunction())
                .keyBy(c->c.orderId()).process(new TradeFactFunction(300000))
                .keyBy(f->f.fact_id).process(new FactDeduplicator()).executeAndCollect()) {
            while(it.hasNext()) {
                ItemFact f=it.next(); count++;
                if("PAYMENT".equals(f.fact_type)) paid+=f.amount_cent; else refunded+=f.amount_cent;
            }
        }
        assertEquals(5,count); assertEquals(30000,paid); assertEquals(30000,refunded);
    }
}
