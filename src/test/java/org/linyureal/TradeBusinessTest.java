package org.linyureal;

import org.junit.jupiter.api.Test;
import org.linyureal.common.Json;
import org.linyureal.function.trade.TradeAssembler;
import org.linyureal.function.metric.DimensionExpander;
import org.linyureal.mock.generator.TradeGenerator;
import org.linyureal.mock.scenario.Scenario;
import org.linyureal.model.cdc.Change;
import org.linyureal.model.metric.*;
import org.linyureal.model.trade.TradeState;
import org.linyureal.parser.ChangeParser;
import java.time.LocalDateTime;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TradeBusinessTest {
    private List<Change> generate(Scenario scenario) {
        List<Change> rows=new ArrayList<>();
        TradeGenerator.generate("TEST",scenario,LocalDateTime.of(2026,8,11,10,0),new Random(1)).forEach(rows::addAll);
        return rows;
    }
    private List<ItemFact> run(List<Change> rows) throws Exception {
        TradeState state=new TradeState(); List<ItemFact> facts=new ArrayList<>();
        for(Change row:rows) {
            TradeState candidate=new TradeState(state);
            facts.addAll(TradeAssembler.accept(candidate,row)); state=candidate;
        }
        return facts;
    }
    private long total(List<ItemFact> facts,String type) {
        return facts.stream().filter(f->type.equals(f.fact_type)).mapToLong(f->f.amount_cent).sum();
    }
    @Test void allBusinessScenarios() throws Exception {
        long[] refunds={0,0,0,3000,5000,30000,0,3000};
        for(Scenario s:Scenario.values()) {
            List<ItemFact> facts=run(generate(s));
            assertEquals(s==Scenario.CANCELLED?0:30000,total(facts,"PAYMENT"),s.name());
            assertEquals(refunds[s.ordinal()],total(facts,"REFUND"),s.name());
        }
    }
    @Test void shuffledDuplicateAndOldVersionsConverge() throws Exception {
        for(Scenario scenario:Scenario.values()) for(int seed=0;seed<50;seed++) {
            List<Change> rows=generate(scenario); List<ItemFact> expected=run(rows);
            rows.addAll(new ArrayList<>(rows)); Collections.shuffle(rows,new Random(seed));
            List<ItemFact> actual=run(rows);
            assertEquals(total(expected,"PAYMENT"),total(actual,"PAYMENT"),scenario+" seed="+seed);
            assertEquals(total(expected,"REFUND"),total(actual,"REFUND"));
            assertEquals(expected.size(),actual.size());
        }
    }
    @Test void incompletePaymentWaitsForLastAllocation() throws Exception {
        List<Change> rows=generate(Scenario.PAID); Change last=rows.stream()
                .filter(c->c.table.equals("trade_payment_item")).findFirst().orElseThrow();
        rows.remove(last); assertTrue(run(rows).isEmpty());
        rows.add(last); assertEquals(30000,total(run(rows),"PAYMENT"));
    }
    @Test void sameVersionConflictDoesNotChangeCommittedState() throws Exception {
        Change first=generate(Scenario.PAID).get(0); TradeState committed=new TradeState();
        TradeAssembler.accept(committed,first);
        String original=committed.rows.values().iterator().next();
        Change bad=new Change(first.table,first.op,first.after.deepCopy()); bad.after.put("user_id","different");
        assertThrows(IllegalArgumentException.class,()->TradeAssembler.accept(new TradeState(committed),bad));
        assertEquals(original,committed.rows.values().iterator().next());
    }
    @Test void rejectsOverRefund() {
        List<Change> rows=generate(Scenario.PARTIAL_REFUND);
        for(Change c:rows) if(c.table.startsWith("trade_refund")) c.after.put("amount_cent",11000);
        assertThrows(IllegalArgumentException.class,()->run(rows));
    }
    @Test void crossDayRefundHasTwoDateMeanings() throws Exception {
        List<MetricDelta> deltas=new ArrayList<>();
        for(ItemFact f:run(generate(Scenario.CROSS_DAY_REFUND))) deltas.addAll(DimensionExpander.expand(f));
        assertEquals(30000,deltas.stream().filter(d->d.dimensionType.equals("PRODUCT")).mapToLong(d->d.paid).sum());
        assertEquals(3000,deltas.stream().filter(d->d.dimensionType.equals("PRODUCT")&&d.bizDate.equals("2026-08-12")).mapToLong(d->d.refunded).sum());
        assertEquals(27000,deltas.stream().filter(d->d.dimensionType.equals("PRODUCT")&&d.bizDate.equals("2026-08-11")).mapToLong(d->d.net).sum());
    }
    @Test void restoredBusinessStateDoesNotEmitAgain() throws Exception {
        TradeState state=new TradeState(); List<Change> rows=generate(Scenario.FULL_REFUND);
        for(Change c:rows) TradeAssembler.accept(state,c);
        TradeState restored=Json.MAPPER.readValue(Json.write(state),TradeState.class);
        for(Change c:rows) assertTrue(TradeAssembler.accept(restored,c).isEmpty());
    }
    @Test void eightScenarioCycleMatchesEveryDimensionFamily() throws Exception {
        Map<String,long[]> sums=new HashMap<>(); int factCount=0;
        for(Scenario scenario:Scenario.values()) {
            List<ItemFact> facts=run(generate(scenario)); factCount+=facts.size();
            for(ItemFact fact:facts) for(MetricDelta delta:DimensionExpander.expand(fact)) {
                long[] sum=sums.computeIfAbsent(delta.dimensionType,k->new long[3]);
                sum[0]+=delta.paid; sum[1]+=delta.refunded; sum[2]+=delta.net;
            }
        }
        assertEquals(21,factCount); assertEquals(7,sums.size());
        sums.forEach((type,sum)->assertArrayEquals(new long[]{210000,41000,169000},sum,type));
    }
    @Test void mockAndDebeziumFormatsAndDeleteBoundary() throws Exception {
        Change c=generate(Scenario.PAID).get(0);
        assertEquals(c.orderId(),ChangeParser.parse(Json.write(c)).orderId());
        com.fasterxml.jackson.databind.node.ObjectNode envelope=Json.MAPPER.createObjectNode();
        envelope.putObject("source").put("table",c.table); envelope.put("op","r");
        envelope.set("after",c.after.deepCopy().put("update_time",1786442400000L));
        assertEquals("2026-08-11 10:00:00",ChangeParser.parse(Json.write(envelope)).after.path("update_time").asText());
        envelope.put("op","d"); envelope.set("before",c.after);
        assertThrows(IllegalArgumentException.class,()->ChangeParser.parse(Json.write(envelope)));
        assertNull(ChangeParser.parse("null"));
    }
}
