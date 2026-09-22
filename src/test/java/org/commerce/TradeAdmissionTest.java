package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.CloseableIterator;
import org.commerce.common.Json;
import org.commerce.common.MetricGroup;
import org.commerce.config.CommerceConfig;
import org.commerce.function.Horizons;
import org.commerce.function.Outputs;
import org.commerce.function.TradeFactParseFunction;
import org.commerce.job.TradePipelines;
import org.commerce.mock.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class TradeAdmissionTest {
    @Test
    void bothSidesOfMidnightAdmissionBoundaryAreExplicit() throws Exception {
        var payment =
                BusinessAndMetricsTest.events("BOUNDARY", Scenario.PAID, 1789185600000L).get(1);
        long expiresAt = Horizons.expires(Horizons.date(payment.occurredAt), 7);
        try (var harness =
                new OneInputStreamOperatorTestHarness<String, String>(
                        new ProcessOperator<>(new TradeFactParseFunction(7)))) {
            harness.open();
            harness.setProcessingTime(expiresAt - 1);
            harness.processElement(Json.write(payment), 0);
            assertEquals(1, harness.extractOutputValues().size());
            harness.getOutput().clear();
            harness.setProcessingTime(expiresAt);
            harness.processElement(Json.write(payment), 0);
            assertTrue(harness.extractOutputValues().isEmpty());
            assertEquals(1, harness.getSideOutput(Outputs.REPAIR).size());
        }
    }

    @Test
    void auditKeepsHistoricalFactsWhileOnlineMetricsRejectFutureClocks() throws Exception {
        var payment = BusinessAndMetricsTest.events("TIME", Scenario.PAID, 1789185600000L).get(1);
        try (var audit =
                        new OneInputStreamOperatorTestHarness<String, String>(
                                new ProcessOperator<>(new TradeFactParseFunction()));
                var metrics =
                        new OneInputStreamOperatorTestHarness<String, String>(
                                new ProcessOperator<>(new TradeFactParseFunction(7)))) {
            audit.open();
            audit.setProcessingTime(payment.occurredAt + 20L * 86400000);
            audit.processElement(Json.write(payment), 0);
            assertEquals(1, audit.extractOutputValues().size());
            metrics.open();
            metrics.setProcessingTime(payment.occurredAt - 300001);
            metrics.processElement(Json.write(payment), 0);
            assertTrue(metrics.extractOutputValues().isEmpty());
            assertEquals(1, metrics.getSideOutput(Outputs.QUALITY).size());
        }
    }

    @ParameterizedTest
    @EnumSource(MetricGroup.class)
    @Timeout(60)
    void actualMetricPipelinesRouteBacklogToRepairBeforeUpdatingState(MetricGroup group)
            throws Exception {
        Properties values = new Properties();
        try (var input = getClass().getResourceAsStream("/commerce/application.properties")) {
            values.load(input);
        }
        long createdAt = System.currentTimeMillis() - 20L * 86400000;
        var payment = BusinessAndMetricsTest.events("BACKLOG", Scenario.PAID, createdAt).get(1);
        var environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        var streams =
                TradePipelines.metrics(
                        environment.fromElements(Json.write(payment)),
                        new CommerceConfig(values),
                        group);
        var observed =
                streams.records
                        .map(raw -> "accepted:" + raw)
                        .union(
                                streams.quality.map(raw -> "quality:" + raw),
                                streams.repair.map(raw -> "repair:" + raw));
        List<String> results = new ArrayList<>();
        try (CloseableIterator<String> iterator = observed.executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }
        assertEquals(1, results.size());
        assertTrue(results.get(0).startsWith("repair:"));
        assertTrue(results.get(0).contains("fact-horizon"));
    }
}
