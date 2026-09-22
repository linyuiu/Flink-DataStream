package org.commerce.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;
import org.commerce.function.OutboxParseFunction;
import org.commerce.source.Connectors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.Random;

class CommerceKafkaSimulatorTest {
    private static final long NOW = 1_789_185_900_000L;

    @Test
    void sharedFactoryPreservesMysqlSimulatorPlansForAllScenarios() {
        Properties properties = defaults();
        properties.setProperty("mock.run.id", "PARITY");
        properties.setProperty("business.payment.rate", "0.20");
        properties.setProperty("business.average.paid.cent", "2740");
        properties.setProperty("mock.seed", "20260914");

        for (String mode : new String[] {"PROFILE", "MATRIX", "PAID", "RETRY_PAID"}) {
            properties.setProperty("mock.scenario", mode);
            MockOrderPlans shared = new MockOrderPlans(new CommerceConfig(properties));
            for (long index = 0; index < 200; index++) {
                List<TransactionPlan> actual = shared.generate(index, NOW);
                List<TransactionPlan> previous = previousMysqlPlans(mode, index);
                assertSamePlans(previous, actual);
            }
        }
    }

    @Test
    void independentSimulatorRunsGenerateIdenticalRowsAndEventsWithAFixedClock() {
        Properties properties = defaults();
        properties.setProperty("mock.run.id", "PARITY");
        properties.setProperty("mock.scenario", "MATRIX");
        properties.setProperty("mock.orders.per.second", "7");
        properties.setProperty("mock.start.time.ms", "1789956000000");

        MockOrderPlans mysqlRun = new MockOrderPlans(new CommerceConfig(properties));
        MockOrderPlans kafkaRun = new MockOrderPlans(new CommerceConfig(properties));
        for (long index = 0; index < 90; index++) {
            // 两个任务分别在不同实际时间启动，业务行、事件ID、金额和时间依旧相同。
            assertSamePlans(
                    mysqlRun.generate(index, NOW), kafkaRun.generate(index, NOW + 3_600_000));
        }
        assertEquals(1789956000000L, mysqlRun.generate(0, NOW).get(0).event.occurredAt);
        assertEquals(1789956000000L + 1_000L, mysqlRun.generate(7, NOW).get(0).event.occurredAt);
    }

    @Test
    void kafkaRecordsMatchTheExistingFactAndDimensionConsumers() {
        Properties properties = defaults();
        properties.setProperty("mock.scenario", "PARTIAL_REFUND");
        CommerceConfig config = new CommerceConfig(properties);
        Connectors.CdcRouter router = new Connectors.CdcRouter(config);

        List<TransactionPlan> plans = new MockOrderPlans(config).generate(7, NOW);
        long emitted = 0;
        for (TransactionPlan plan : plans) {
            if (plan.event == null) {
                continue;
            }
            emitted++;
            String envelope =
                    MockCdcRecord.insert("trade_outbox", MockCdcRecord.outboxRow(plan.event));
            assertEquals(
                    Json.write(plan.event),
                    Json.text(Json.object(envelope).path("after"), "payload_json"));
            assertNotNull(OutboxParseFunction.parse(envelope));
            ProducerRecord<byte[], byte[]> record = router.serialize(envelope, null, null);
            assertEquals(config.topic("trade_outbox"), record.topic());
            assertEquals(plan.event.orderId, new String(record.key(), StandardCharsets.UTF_8));
        }
        assertEquals(3, emitted); // 下单、成功支付、一次成功部分退款。

        ObjectNode sku = Catalog.rows().get("dim_sku").get(0);
        String envelope = MockCdcRecord.insert("dim_sku", sku);
        ProducerRecord<byte[], byte[]> record = router.serialize(envelope, null, null);
        assertEquals(config.topic("dim_sku"), record.topic());
        assertEquals(sku.path("id").asText(), new String(record.key(), StandardCharsets.UTF_8));
        assertEquals(sku, Json.object(envelope).path("after"));
    }

    @Test
    void failedPaymentAndFailedRefundDoNotProduceSuccessFacts() {
        Properties properties = defaults();
        properties.setProperty("mock.scenario", "PAYMENT_FAILED");
        MockOrderPlans paymentFailed = new MockOrderPlans(new CommerceConfig(properties));
        List<TransactionPlan> paymentPlans = paymentFailed.generate(0, NOW);
        assertEquals(1, paymentPlans.stream().filter(plan -> plan.event != null).count());
        assertFalse(
                paymentPlans.stream()
                        .anyMatch(
                                plan ->
                                        plan.rows.stream()
                                                .anyMatch(row -> row.table.equals("pay_ledger"))));

        properties.setProperty("mock.scenario", "REFUND_FAILED");
        MockOrderPlans refundFailed = new MockOrderPlans(new CommerceConfig(properties));
        List<TransactionPlan> refundPlans = refundFailed.generate(0, NOW);
        assertEquals(2, refundPlans.stream().filter(plan -> plan.event != null).count());
        assertFalse(
                refundPlans.stream()
                        .anyMatch(
                                plan ->
                                        plan.event != null
                                                && plan.event.eventType.equals(
                                                        "REFUND_SUCCEEDED")));
    }

    /** 直接复写原MySQL模拟器中的抽样步骤，检测共享生成器是否改变了既有数据。 */
    private static List<TransactionPlan> previousMysqlPlans(String mode, long index) {
        Random random = new Random(20260914L + index);
        Scenario scenario = previousScenario(mode, index, random);
        long goodsCent = Math.max(200, Math.round(2740 * (.6 + .8 * random.nextDouble())));
        long createdAt = NOW - 300_000;
        if (scenario == Scenario.OLD_ORDER_REFUND) {
            createdAt -= 14L * 86_400_000;
        }
        return BusinessGenerator.generate(
                "PARITY-" + index, scenario, createdAt, goodsCent, random);
    }

    private static Scenario previousScenario(String mode, long index, Random random) {
        if (mode.equals("MATRIX")) {
            return Scenario.values()[(int) (index % Scenario.values().length)];
        }
        if (!mode.equals("PROFILE")) {
            return Scenario.valueOf(mode);
        }
        if (random.nextDouble() >= 0.20) {
            return random.nextDouble() < .8 ? Scenario.CANCELLED : Scenario.PAYMENT_FAILED;
        }
        double sample = random.nextDouble();
        if (sample < .05) {
            return Scenario.PARTIAL_REFUND;
        }
        if (sample < .10) {
            return Scenario.MULTIPLE_REFUNDS;
        }
        if (sample < .15) {
            return Scenario.FULL_GOODS_REFUND;
        }
        if (sample < .18) {
            return Scenario.REFUND_FAILED;
        }
        if (sample < .28) {
            return Scenario.RETRY_PAID;
        }
        return Scenario.PAID;
    }

    private static void assertSamePlans(
            List<TransactionPlan> expected, List<TransactionPlan> actual) {
        assertEquals(expected.size(), actual.size());
        for (int planIndex = 0; planIndex < expected.size(); planIndex++) {
            TransactionPlan before = expected.get(planIndex);
            TransactionPlan after = actual.get(planIndex);
            assertEquals(Json.write(before.event), Json.write(after.event));
            assertEquals(before.rows.size(), after.rows.size());
            for (int rowIndex = 0; rowIndex < before.rows.size(); rowIndex++) {
                TransactionPlan.Row oldRow = before.rows.get(rowIndex);
                TransactionPlan.Row newRow = after.rows.get(rowIndex);
                assertEquals(oldRow.table, newRow.table);
                assertEquals(oldRow.update, newRow.update);
                assertEquals(oldRow.value, newRow.value);
            }
        }
    }

    private static Properties defaults() {
        Properties values = new Properties();
        try (var input =
                CommerceKafkaSimulatorTest.class.getResourceAsStream(
                        "/commerce/application.properties")) {
            assertNotNull(input);
            values.load(input);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        return values;
    }
}
