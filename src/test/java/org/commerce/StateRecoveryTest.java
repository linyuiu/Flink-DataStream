package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.commerce.common.Json;
import org.commerce.function.*;
import org.commerce.mock.Scenario;
import org.commerce.model.*;
import org.junit.jupiter.api.Test;

import java.time.*;

class StateRecoveryTest {
    private final long now = Instant.parse("2026-09-14T02:00:00Z").toEpochMilli();

    private KeyedOneInputStreamOperatorTestHarness<String, String, String> dedup()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AdmitAndDeduplicate(7)),
                raw -> Json.text(Json.object(raw), "eventId"),
                Types.STRING);
    }

    @Test
    void restoredFingerprintRejectsReplayAndConflictWithoutMillionsOfTimers() throws Exception {
        String event =
                Json.write(
                        BusinessAndMetricsTest.events("RESTORE", Scenario.PAID, now - 300000)
                                .get(0));
        OperatorSubtaskState saved;
        try (var h = dedup()) {
            h.open();
            h.setProcessingTime(now);
            h.setStateTtlProcessingTime(now);
            h.processElement(event, 0);
            assertEquals(1, h.extractOutputValues().size());
            assertEquals(0, h.numProcessingTimeTimers());
            saved = h.snapshot(1, now);
        }
        try (var h = dedup()) {
            h.initializeState(saved);
            h.open();
            h.setProcessingTime(now + 1000);
            h.setStateTtlProcessingTime(now + 1000);
            h.processElement(event, 0);
            assertTrue(h.extractOutputValues().isEmpty());
            var changed = Json.object(event);
            changed.put("userId", "changed-user");
            h.processElement(changed.toString(), 0);
            assertEquals(1, h.getSideOutput(Outputs.QUALITY).size());
        }
    }

    @Test
    void expiredFingerprintDoesNotMakeOldEventCountAgain() throws Exception {
        String event =
                Json.write(
                        BusinessAndMetricsTest.events("TTL", Scenario.PAID, now - 300000).get(0));
        try (var h = dedup()) {
            h.open();
            h.setProcessingTime(now);
            h.setStateTtlProcessingTime(now);
            h.processElement(event, 0);
            h.getOutput().clear();
            long later = now + 10L * 86400000;
            h.setProcessingTime(later);
            h.setStateTtlProcessingTime(later);
            h.processElement(event, 0);
            assertTrue(h.extractOutputValues().isEmpty());
            assertEquals(1, h.getSideOutput(Outputs.REPAIR).size());
        }
    }

    @Test
    void exactDistinctSurvivesCheckpointAndRejectsExpiredDay() throws Exception {
        TradeEvent paid = BusinessAndMetricsTest.events("USER", Scenario.PAID, now - 300000).get(1);
        MetricDelta user =
                EventExpansion.expand(paid, 32, 8, 4).stream()
                        .filter(
                                d ->
                                        d.dimensionType.equals("ALL")
                                                && "PAID_USER".equals(d.distinctKind))
                        .findFirst()
                        .orElseThrow();
        OperatorSubtaskState snapshot;
        try (var h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new ExactDistinctFunction(7)),
                        MetricDelta::distinctKey,
                        Types.STRING)) {
            h.open();
            h.setProcessingTime(now);
            h.setStateTtlProcessingTime(now);
            h.processElement(user, 0);
            assertEquals(1, h.extractOutputValues().get(0).values[3]);
            snapshot = h.snapshot(1, now);
        }
        try (var h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new ExactDistinctFunction(7)),
                        MetricDelta::distinctKey,
                        Types.STRING)) {
            h.initializeState(snapshot);
            h.open();
            h.setProcessingTime(now);
            h.setStateTtlProcessingTime(now);
            h.processElement(user, 0);
            assertTrue(h.extractOutputValues().isEmpty());
            long closed = Horizons.expires(user.date, 7);
            h.setProcessingTime(closed);
            h.processElement(user, 0);
            assertEquals(1, h.getSideOutput(Outputs.REPAIR).size());
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<String, MetricDelta, String> accumulator()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new ShardedAccumulator(93, 100)),
                MetricDelta::partitionKey,
                Types.STRING);
    }

    @Test
    void partialTotalsAndVersionRecoverThenClosedDayCannotBeOverwritten() throws Exception {
        TradeEvent paid =
                BusinessAndMetricsTest.events("TOTAL", Scenario.PAID, now - 300000).get(1);
        MetricDelta delta =
                EventExpansion.expand(paid, 32, 8, 4).stream()
                        .filter(d -> d.dimensionType.equals("ALL") && d.distinctKind == null)
                        .findFirst()
                        .orElseThrow();
        OperatorSubtaskState snapshot;
        try (var h = accumulator()) {
            h.open();
            h.setProcessingTime(now);
            h.processElement(delta, 0);
            h.setProcessingTime(now + 100);
            assertEquals(
                    2740,
                    Json.object(h.extractOutputValues().get(0)).path("paid_goods_cent").asLong());
            snapshot = h.snapshot(1, now + 100);
        }
        try (var h = accumulator()) {
            h.initializeState(snapshot);
            h.open();
            h.setProcessingTime(now + 200);
            MetricDelta correction = new MetricDelta();
            correction.date = delta.date;
            correction.dimensionType = delta.dimensionType;
            correction.dimensionId = delta.dimensionId;
            correction.shard = delta.shard;
            correction.values = new long[11];
            correction.values[10] = -100;
            h.processElement(correction, 0);
            h.setProcessingTime(now + 300);
            var row = Json.object(h.extractOutputValues().get(0));
            assertEquals(2640, row.path("net_paid_goods_cent").asLong());
            assertEquals(2, row.path("update_seq").asLong());
            h.getOutput().clear();
            h.setProcessingTime(Horizons.expires(delta.date, 93));
            h.processElement(correction, 0);
            assertTrue(h.extractOutputValues().isEmpty());
            assertEquals(1, h.getSideOutput(Outputs.REPAIR).size());
        }
    }
}
