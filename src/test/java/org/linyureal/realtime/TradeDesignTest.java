package org.linyureal.realtime;

import org.junit.jupiter.api.Test;
import org.linyureal.realtime.common.*;
import org.linyureal.realtime.function.*;
import org.linyureal.realtime.mock.*;
import org.linyureal.realtime.model.*;
import org.linyureal.realtime.validation.RowValidator;
import java.time.LocalDateTime;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TradeDesignTest {
    static List<RowChange> rows(Scenario scenario, String orderId) {
        List<RowChange> result = new ArrayList<>();
        TransactionGenerator.generate(orderId, scenario, LocalDateTime.of(2026, 9, 12, 10, 0), new Random(19)).forEach(result::addAll);
        return result;
    }
    static List<TradeFact> run(List<RowChange> rows) {
        OrderState state = new OrderState(); List<TradeFact> facts = new ArrayList<>();
        for (RowChange row : rows) {
            OrderState candidate = new OrderState(state); facts.addAll(TradeProjector.accept(candidate, row)); state = candidate;
        }
        return facts;
    }
    static long amount(List<TradeFact> facts, String type) {
        return facts.stream().filter(f -> f.fact_type.equals(type)).mapToLong(f -> f.amount_cent).sum();
    }
    @Test void nineScenariosRespectSuccessfulMoneyAndFailureRecords() {
        for (Scenario scenario : Scenario.values()) {
            List<RowChange> rows = rows(scenario, scenario.name()); rows.forEach(RowValidator::validate);
            List<TradeFact> facts = run(rows);
            assertEquals(2, facts.stream().filter(f -> f.fact_type.equals("CREATED")).count());
            long goods = Jsons.integer(Jsons.object(rows.get(0).rowJson), "goods_cent");
            if (scenario == Scenario.PAYMENT_FAILED || scenario == Scenario.CANCELLED) assertEquals(0, amount(facts, "PAID"));
            else assertEquals(goods, amount(facts, "PAID"));
            if (scenario == Scenario.REFUND_FAILED) assertEquals(0, amount(facts, "REFUNDED"));
            if (scenario == Scenario.FULL_GOODS_REFUND) assertEquals(goods, amount(facts, "REFUNDED"));
            if (scenario == Scenario.PAYMENT_FAILED) {
                assertTrue(rows.stream().anyMatch(r -> r.table.equals("payment_attempt") && Jsons.object(r.rowJson).path("status").asText().equals("FAILED")));
                assertFalse(rows.stream().anyMatch(r -> r.table.equals("payment_ledger")));
            }
        }
    }
    @Test void shuffledRepeatedCdcConvergesAcross450Cases() {
        for (Scenario scenario : Scenario.values()) for (int seed = 0; seed < 50; seed++) {
            List<RowChange> rows = rows(scenario, scenario.name());
            Map<String, String> expected = new TreeMap<>(); run(rows).forEach(f -> expected.put(f.fact_id, Jsons.write(f)));
            rows.addAll(new ArrayList<>(rows)); Collections.shuffle(rows, new Random(seed));
            Map<String, String> actual = new TreeMap<>(); List<TradeFact> output = run(rows);
            output.forEach(f -> actual.put(f.fact_id, Jsons.write(f)));
            assertEquals(expected, actual, scenario + "/" + seed); assertEquals(actual.size(), output.size());
        }
    }
    @Test void itemRowsDoNotInflateOrderAndUserCounts() {
        List<TradeFact> facts = run(rows(Scenario.MULTIPLE_REFUNDS, "O1"));
        Map<String, long[]> totals = aggregate(facts);
        long[] global = totals.get("2026-09-12|ALL|ALL");
        assertEquals(1, global[0]); assertEquals(1, global[2]); assertEquals(1, global[3]);
        assertEquals(1, global[4]); assertEquals(2, global[5]);
        assertEquals(amount(facts, "PAID") - amount(facts, "REFUNDED"), global[10]);
        assertEquals(2, totals.entrySet().stream().filter(e -> e.getKey().contains("|PRODUCT|")).mapToLong(e -> e.getValue()[2]).sum());
    }
    @Test void crossDayRefundSeparatesEventDateAndPayDate() {
        List<TradeFact> facts = run(rows(Scenario.CROSS_DAY_REFUND, "X")); Map<String, long[]> totals = aggregate(facts);
        long paid = amount(facts, "PAID"), refunded = amount(facts, "REFUNDED");
        assertEquals(paid, totals.get("2026-09-12|ALL|ALL")[8]);
        assertEquals(0, totals.get("2026-09-12|ALL|ALL")[9]);
        assertEquals(paid - refunded, totals.get("2026-09-12|ALL|ALL")[10]);
        assertEquals(refunded, totals.get("2026-09-13|ALL|ALL")[9]);
        assertEquals(0, totals.get("2026-09-13|ALL|ALL")[10]);
    }
    @Test void missingPaymentLineWaitsWithoutLosingCreatedFacts() {
        List<RowChange> rows = rows(Scenario.PAID, "LATE");
        RowChange last = rows.stream().filter(c -> c.table.equals("payment_line")).findFirst().orElseThrow();
        rows.remove(last); assertEquals(0, amount(run(rows), "PAID"));
        OrderState waiting = new OrderState();
        for (RowChange row : rows) TradeProjector.accept(waiting, row);
        assertEquals("Missing payment allocations", TradeProjector.incompleteReason(waiting));
        TradeProjector.accept(waiting, last); assertNull(TradeProjector.incompleteReason(waiting));
        rows.add(last); assertTrue(amount(run(rows), "PAID") > 0);
    }
    @Test void conflictsAndOverRefundRejectCandidateState() {
        List<RowChange> rows = rows(Scenario.PARTIAL_REFUND, "BAD"); OrderState state = new OrderState();
        RowChange first = rows.get(0); TradeProjector.accept(state, first); String committed = Jsons.write(state);
        RowChange conflict = new RowChange(first.table, first.operation, first.orderId,
                Jsons.object(first.rowJson).put("user_id", "CONFLICT").toString());
        assertThrows(IllegalArgumentException.class, () -> TradeProjector.accept(new OrderState(state), conflict));
        assertEquals(committed, Jsons.write(state));
        for (RowChange c : rows) if (c.table.startsWith("refund_")) c.rowJson = Jsons.object(c.rowJson).put("amount_cent", 1000000).toString();
        assertThrows(IllegalArgumentException.class, () -> run(rows));
    }
    @Test void businessStateRoundTripDoesNotReemit() throws Exception {
        List<RowChange> rows = rows(Scenario.FULL_GOODS_REFUND, "RESTORE"); OrderState state = new OrderState();
        for (RowChange row : rows) TradeProjector.accept(state, row);
        OrderState restored = Jsons.MAPPER.readValue(Jsons.write(state), OrderState.class);
        for (RowChange row : rows) assertTrue(TradeProjector.accept(restored, row).isEmpty());
    }
    @Test void couponAllocationsAndCatalogReferencesHoldAcrossSeeds() {
        for (int seed = 0; seed < 100; seed++) {
            List<RowChange> rows = new ArrayList<>();
            TransactionGenerator.generate("PRICE", Scenario.PAID, LocalDateTime.of(2026, 9, 12, 10, 0), new Random(seed)).forEach(rows::addAll);
            List<TradeFact> facts = run(rows);
            long goods = Jsons.integer(Jsons.object(rows.get(0).rowJson), "goods_cent");
            assertEquals(goods, amount(facts, "PAID"));
            for (TradeFact f : facts) {
                var sku = Catalog.rows().get("dim_sku").stream().filter(n -> n.path("id").asText().equals(f.sku_id)).findFirst().orElseThrow();
                assertEquals(f.product_id, sku.path("product_id").asText());
            }
        }
    }
    static Map<String, long[]> aggregate(List<TradeFact> facts) {
        Map<String, long[]> totals = new HashMap<>(); Map<String, Set<String>> distinct = new HashMap<>();
        for (TradeFact f : facts) for (MetricInput in : MetricLogic.expand(MetricLogic.parse(Jsons.write(f)))) {
            Set<String> seen = distinct.computeIfAbsent(in.key(), k -> new HashSet<>());
            totals.put(in.key(), MetricLogic.add(totals.getOrDefault(in.key(), new long[Contracts.METRICS.length]), in, seen::add));
        }
        return totals;
    }
}
