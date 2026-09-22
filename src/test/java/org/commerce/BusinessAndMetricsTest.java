package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import org.commerce.common.*;
import org.commerce.function.*;
import org.commerce.mock.*;
import org.commerce.model.*;
import org.commerce.validation.EventContract;
import org.junit.jupiter.api.Test;

import java.util.*;

class BusinessAndMetricsTest {
    static List<TradeEvent> events(String order, Scenario scenario, long created) {
        List<TradeEvent> events = new ArrayList<>();
        for (TransactionPlan p :
                BusinessGenerator.generate(order, scenario, created, 2740, new Random(1)))
            if (p.event != null) events.add(p.event);
        return events;
    }

    static Map<String, long[]> aggregate(List<TradeEvent> events) {
        Map<String, long[]> result = new HashMap<>();
        Set<String> ids = new HashSet<>(), distinct = new HashSet<>();
        for (TradeEvent e : events) {
            if (!ids.add(e.eventId)) continue;
            for (MetricDelta d : EventExpansion.expand(e, 32, 8, 4)) {
                if (d.distinctKind != null) {
                    if (!distinct.add(d.distinctKey())) continue;
                    d.values[d.distinctKind.equals("PAID_USER") ? 3 : 4] = 1;
                }
                long[] total =
                        result.computeIfAbsent(
                                d.date + "|" + d.dimensionType + "|" + d.dimensionId,
                                k -> new long[Contract.METRICS.length]);
                for (int i = 0; i < total.length; i++) total[i] += d.values[i];
            }
        }
        return result;
    }

    @Test
    void moneyStatesAndFailedAttemptsAreSeparated() {
        for (Scenario s : Scenario.values()) {
            List<TransactionPlan> plans =
                    BusinessGenerator.generate("O", s, 1789185600000L, 2740, new Random(2));
            List<TradeEvent> events = new ArrayList<>();
            plans.forEach(
                    p -> {
                        if (p.event != null) events.add(p.event);
                    });
            events.forEach(EventContract::validate);
            long paid =
                    events.stream()
                            .filter(e -> e.eventType.equals("PAYMENT_SUCCEEDED"))
                            .mapToLong(e -> e.goodsCent)
                            .sum();
            long refunded =
                    events.stream()
                            .filter(e -> e.eventType.equals("REFUND_SUCCEEDED"))
                            .mapToLong(e -> e.goodsCent)
                            .sum();
            assertEquals(
                    s == Scenario.CANCELLED || s == Scenario.PAYMENT_FAILED ? 0 : 2740,
                    paid,
                    s.name());
            if (s == Scenario.FULL_GOODS_REFUND) assertEquals(paid, refunded);
            if (s == Scenario.REFUND_FAILED) assertEquals(0, refunded);
            if (s == Scenario.PAYMENT_FAILED) {
                assertTrue(
                        plans.stream()
                                .flatMap(p -> p.rows.stream())
                                .anyMatch(
                                        r ->
                                                r.table.equals("pay_attempt")
                                                        && r.value
                                                                .path("status")
                                                                .asText()
                                                                .equals("FAILED")));
                assertFalse(
                        plans.stream()
                                .flatMap(p -> p.rows.stream())
                                .anyMatch(r -> r.table.equals("pay_ledger")));
            }
        }
    }

    @Test
    void twoLinesCountOneOrderAndRefundRequestsDoNotInflateRefundOrders() {
        long t = 1789185600000L;
        String date = Horizons.date(t);
        var totals = aggregate(events("O", Scenario.MULTIPLE_REFUNDS, t));
        long[] all = totals.get(date + "|ALL|ALL");
        assertEquals(1, all[0]);
        assertEquals(1, all[2]);
        assertEquals(1, all[3]);
        assertEquals(1, all[4]);
        assertEquals(2, all[5]);
        assertEquals(2740, all[8]);
        assertEquals(all[8] - all[9], all[10]);
    }

    @Test
    void refundsUseOriginalPayDayForNetAndOccurrenceDayForRefundAmount() {
        long created = 1789185600000L;
        List<TradeEvent> events = events("O", Scenario.OLD_ORDER_REFUND, created);
        TradeEvent refund =
                events.stream()
                        .filter(e -> e.eventType.equals("REFUND_SUCCEEDED"))
                        .findFirst()
                        .orElseThrow();
        var totals = aggregate(events);
        var before = totals.get(Horizons.date(created) + "|ALL|ALL");
        var today = totals.get(Horizons.date(refund.occurredAt) + "|ALL|ALL");
        assertEquals(2740 - refund.goodsCent, before[10]);
        assertEquals(0, before[9]);
        assertEquals(refund.goodsCent, today[9]);
        assertEquals(0, today[10]);
    }

    @Test
    void dedupAndRandomOrderPreserveTotalsAcross450Sequences() {
        for (Scenario scenario : Scenario.values()) {
            var original = events("SHUFFLE", scenario, 1789185600000L);
            var expected = aggregate(original);
            for (int seed = 0; seed < 50; seed++) {
                var mixed = new ArrayList<>(original);
                mixed.addAll(original);
                Collections.shuffle(mixed, new Random(seed));
                var result = aggregate(mixed);
                assertEquals(expected.keySet(), result.keySet());
                expected.forEach((k, v) -> assertArrayEquals(v, result.get(k), k));
            }
        }
    }

    @Test
    void userCountsRemainExactAcrossOrdersAndShardsSpreadGlobalLoad() {
        List<TradeEvent> events = new ArrayList<>();
        Set<Integer> shards = new HashSet<>();
        for (int i = 0; i < 500; i++)
            for (TradeEvent e : events("O" + i, Scenario.PAID, 1789185600000L)) {
                e.userId = "same-user";
                events.add(e);
                for (MetricDelta d : EventExpansion.expand(e, 32, 8, 4))
                    if (d.dimensionType.equals("ALL") && d.distinctKind == null)
                        shards.add(d.shard);
            }
        var totals = aggregate(events).get(Horizons.date(1789185600000L) + "|ALL|ALL");
        assertEquals(500, totals[2]);
        assertEquals(1, totals[3]);
        assertTrue(shards.size() >= 24);
    }

    @Test
    void malformedAmountsAndDuplicateLinesRejectBeforeState() {
        TradeEvent e = events("B", Scenario.PAID, 1789185600000L).get(0);
        e.goodsCent++;
        assertThrows(IllegalArgumentException.class, () -> EventContract.validate(e));
        e.goodsCent--;
        e.lines.add(e.lines.get(0));
        assertThrows(IllegalArgumentException.class, () -> EventContract.validate(e));
    }

    @Test
    void horizonsCloseAtFixedBusinessMidnight() {
        long end = Horizons.expires("2026-09-01", 7);
        assertTrue(Horizons.open("2026-09-01", end - 1, 7));
        assertFalse(Horizons.open("2026-09-01", end, 7));
        assertFalse(Horizons.open("2026-09-01", end + 30L * 86400000, 7));
    }

    @Test
    void outboxContractRejectsUpdatesAndMismatch() {
        TradeEvent e = events("CDC", Scenario.PAID, 1789185600000L).get(0);
        var root = Json.MAPPER.createObjectNode();
        root.putObject("source").put("table", "trade_outbox");
        root.put("op", "r");
        root.putObject("after")
                .put("id", e.eventId)
                .put("order_id", e.orderId)
                .put("event_type", e.eventType)
                .put("business_id", e.businessId)
                .put("occurred_at", e.occurredAt)
                .put("payload_json", Json.write(e));
        assertEquals(
                e.eventId, EventContract.parse(OutboxParseFunction.parse(root.toString())).eventId);
        root.put("op", "u");
        assertThrows(
                IllegalArgumentException.class, () -> OutboxParseFunction.parse(root.toString()));
    }
}
