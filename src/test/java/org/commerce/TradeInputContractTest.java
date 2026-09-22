package org.commerce;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;
import org.commerce.function.OutboxParseFunction;
import org.commerce.function.Outputs;
import org.commerce.mock.Scenario;
import org.commerce.model.TradeEvent;
import org.commerce.source.Connectors;
import org.commerce.validation.EventContract;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

class TradeInputContractTest {
    private static TradeEvent event() {
        return BusinessAndMetricsTest.events("STRICT-ORDER", Scenario.PAID, 1789185600000L).get(1);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "fraction",
                "string",
                "null",
                "missing-time",
                "missing-schema",
                "numeric-id",
                "fraction-quantity",
                "overflow"
            })
    void rejectsAmbiguousJsonBeforeMoneyEntersState(String invalidCase) {
        ObjectNode json = Json.object(Json.write(event()));
        switch (invalidCase) {
            case "fraction":
                json.put("goodsCent", 2740.9);
                break;
            case "string":
                json.put("goodsCent", "2740");
                break;
            case "null":
                json.putNull("paidAt");
                break;
            case "missing-time":
                json.remove("paidAt");
                break;
            case "missing-schema":
                json.remove("schemaVersion");
                break;
            case "numeric-id":
                json.put("orderId", 123);
                break;
            case "fraction-quantity":
                ((ObjectNode) json.path("lines").get(0)).put("quantity", 1.5);
                break;
            case "overflow":
                json.put("goodsCent", new java.math.BigInteger("9223372036854775808"));
                break;
            default:
                throw new AssertionError(invalidCase);
        }
        assertThrows(IllegalArgumentException.class, () -> EventContract.parse(json.toString()));
    }

    @Test
    void rejectsDuplicateJsonFieldsAndTrailingDocuments() {
        String valid = Json.write(event());
        String duplicate =
                valid.replace("\"goodsCent\":2740", "\"goodsCent\":2740,\"goodsCent\":2740");
        assertNotEquals(valid, duplicate);
        assertThrows(IllegalArgumentException.class, () -> EventContract.parse(duplicate));
        assertThrows(IllegalArgumentException.class, () -> EventContract.parse(valid + " {}"));
        assertEquals(2740, EventContract.parse(valid).goodsCent);
    }

    @Test
    void cleanupDeleteDoesNotEmitRetractionButMutationIsVisible() throws Exception {
        ObjectNode delete = Json.MAPPER.createObjectNode().put("op", "d");
        delete.putObject("source").put("table", "trade_outbox");
        try (var harness =
                new OneInputStreamOperatorTestHarness<String, String>(
                        new ProcessOperator<>(new OutboxParseFunction()))) {
            harness.open();
            harness.processElement(delete.toString(), 0);
            assertTrue(harness.extractOutputValues().isEmpty());
            assertNull(harness.getSideOutput(Outputs.QUALITY));
            delete.put("op", "u");
            harness.processElement(delete.toString(), 0);
            assertEquals(1, harness.getSideOutput(Outputs.QUALITY).size());
            assertTrue(harness.extractOutputValues().isEmpty());
        }
    }

    @Test
    void kafkaFactsUseOrderIdentityAndDimensionsKeepTheirPrimaryKey() {
        Properties values = new Properties();
        values.setProperty("topic.trade_outbox", "orders");
        values.setProperty("topic.dim_sku", "skus");
        var router = new Connectors.CdcRouter(new CommerceConfig(values));
        ObjectNode envelope = Json.MAPPER.createObjectNode().put("op", "c");
        envelope.putObject("source").put("table", "trade_outbox");
        envelope.putObject("after").put("id", "event-1").put("order_id", "order-1");
        assertEquals(
                "order-1",
                new String(
                        router.serialize(envelope.toString(), null, null).key(),
                        StandardCharsets.UTF_8));
        envelope.put("op", "d");
        envelope.set("before", envelope.remove("after"));
        assertEquals(
                "order-1",
                new String(
                        router.serialize(envelope.toString(), null, null).key(),
                        StandardCharsets.UTF_8));
        envelope.putObject("source").put("table", "dim_sku");
        envelope.putObject("before").put("id", "SKU-1");
        assertEquals(
                "SKU-1",
                new String(
                        router.serialize(envelope.toString(), null, null).key(),
                        StandardCharsets.UTF_8));

        var serializer = new Connectors.TradeFactSerializer("facts");
        var result = serializer.serialize(Json.write(event()), null, null);
        assertEquals("facts", result.topic());
        assertEquals("STRICT-ORDER", new String(result.key(), StandardCharsets.UTF_8));
    }

    @Test
    void productionRejectsLocalCheckpointsAndInsufficientTransactionBudget() throws Exception {
        Properties values = defaults();
        values.setProperty("deployment.environment", "production");
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommerceConfig(values).validateJob("facts"));
        values.setProperty("checkpoint.directory", "s3://test-checkpoints/commerce");
        values.setProperty("kafka.replicas", "3");
        values.setProperty("kafka.min.insync.replicas", "2");
        assertDoesNotThrow(() -> new CommerceConfig(values).validateJob("facts"));
        values.setProperty("checkpoint.facts.interval.ms", "700000");
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommerceConfig(values).validateJob("facts"));
    }

    @Test
    void rejectsParallelismThatCannotFitKeyGroups() throws Exception {
        Properties values = defaults();
        values.setProperty("parallelism.amounts", "129");
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommerceConfig(values).validateJob("amounts"));
        values.setProperty("parallelism.amounts", "8");
        values.setProperty("parallelism.doris", "129");
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommerceConfig(values).validateJob("amounts"));
    }

    private static Properties defaults() throws Exception {
        Properties values = new Properties();
        try (InputStream input =
                TradeInputContractTest.class.getResourceAsStream(
                        "/commerce/application.properties")) {
            values.load(input);
        }
        return values;
    }

    @Test
    void diagnosticsPreserveEvidenceAndIdentifyResponsibleJob() {
        String record = Outputs.record("fact-horizon", "original-fact", "outside online horizon");
        ObjectNode tagged = Json.object(Outputs.forJob(record, "amounts"));
        assertEquals("amounts", tagged.path("job").asText());
        assertEquals("fact-horizon", tagged.path("stage").asText());
        assertEquals("original-fact", tagged.path("raw").asText());
        assertEquals(Json.object(record).get("observed_at"), tagged.get("observed_at"));
    }
}
