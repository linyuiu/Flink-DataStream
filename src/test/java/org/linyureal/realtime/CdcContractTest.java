package org.linyureal.realtime;

import org.junit.jupiter.api.Test;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.mock.Scenario;
import org.linyureal.realtime.parser.CdcRows;
import org.linyureal.realtime.sink.CdcTopicSerializer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CdcContractTest {
    @Test void actualCdcJsonSerializerAndKafkaRoutingPreserveSnapshotAndDate() throws Exception {
        var row = Jsons.object(TradeDesignTest.rows(Scenario.PAID, "CDC").get(0).rowJson);
        SchemaBuilder afterBuilder = SchemaBuilder.struct();
        row.fields().forEachRemaining(e -> afterBuilder.field(e.getKey(), e.getValue().isIntegralNumber()
                ? Schema.INT64_SCHEMA : e.getValue().isNull() ? Schema.OPTIONAL_STRING_SCHEMA
                : e.getKey().endsWith("_at") ? Timestamp.SCHEMA : Schema.STRING_SCHEMA));
        Schema afterSchema = afterBuilder.build(); Struct after = new Struct(afterSchema);
        row.fields().forEachRemaining(e -> {
            Object value = e.getValue().isNull() ? null : e.getValue().isIntegralNumber() ? e.getValue().longValue()
                    : e.getKey().endsWith("_at") ? java.util.Date.from(Jsons.time(row, e.getKey()).toInstant(java.time.ZoneOffset.UTC)) : e.getValue().asText();
            after.put(e.getKey(), value);
        });
        Schema sourceSchema = SchemaBuilder.struct().field("table", Schema.STRING_SCHEMA).build();
        Schema envelope = SchemaBuilder.struct().field("source", sourceSchema).field("op", Schema.STRING_SCHEMA).field("after", afterSchema).build();
        Struct payload = new Struct(envelope).put("source", new Struct(sourceSchema).put("table", "order_header")).put("op", "r").put("after", after);
        SourceRecord record = new SourceRecord(Map.of("server", "unit"), Map.of("pos", 1), "unused", envelope, payload);
        List<String> results = new ArrayList<>();
        new JsonDebeziumDeserializationSchema(false).deserialize(record, new Collector<String>() {
            public void collect(String value) { results.add(value); }
            public void close() {}
        });
        assertEquals(1, results.size());
        String json = results.get(0); var change = CdcRows.trade(json);
        assertEquals("2026-09-12 10:00:00", Jsons.object(change.rowJson).path("created_at").asText());
        Properties properties = new Properties(); properties.put("topic.order_header", "unit.ods.orders");
        var kafka = new CdcTopicSerializer(new PipelineConfig(properties)).serialize(json, null, null);
        assertEquals("unit.ods.orders", kafka.topic()); assertEquals("CDC", new String(kafka.key(), java.nio.charset.StandardCharsets.UTF_8));
    }
    @Test void deletesAndInvalidDatesAreNotSilentlyAccepted() {
        var row = Jsons.object(TradeDesignTest.rows(Scenario.PAID, "DELETE").get(0).rowJson);
        var envelope = Jsons.MAPPER.createObjectNode(); envelope.putObject("source").put("table", "order_header");
        envelope.put("op", "d").set("before", row);
        assertThrows(IllegalArgumentException.class, () -> CdcRows.trade(envelope.toString()));
        envelope.put("op", "c").set("after", row.put("created_at", "2026-02-30 00:00:00"));
        assertThrows(RuntimeException.class, () -> CdcRows.trade(envelope.toString()));
    }
}
