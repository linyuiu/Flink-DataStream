package org.linyureal.realtime.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.linyureal.realtime.common.*;
import org.linyureal.realtime.config.PipelineConfig;
import java.nio.charset.StandardCharsets;

/** Preserve the Debezium envelope. Kafka key is source table PK, not the order association key. */
public class CdcTopicSerializer implements KafkaRecordSerializationSchema<String> {
    private final PipelineConfig config;
    public CdcTopicSerializer(PipelineConfig config) { this.config = config; }
    @Override public ProducerRecord<byte[], byte[]> serialize(String raw, KafkaSinkContext context, Long timestamp) {
        JsonNode p = Jsons.object(raw);
        if (p.has("payload")) p = p.get("payload");
        String table = Jsons.text(p.path("source"), "table");
        if (!Contracts.TRADE_TABLES.contains(table) && !Contracts.DIM_TABLES.contains(table))
            throw new IllegalArgumentException("Unknown CDC table: " + table);
        String op = Jsons.text(p, "op");
        JsonNode row = p.path(op.equals("d") ? "before" : "after");
        byte[] key = Jsons.text(row, "id").getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(config.topic(table), key, raw.getBytes(StandardCharsets.UTF_8));
    }
}
