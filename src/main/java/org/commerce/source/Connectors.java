package org.commerce.source;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.commerce.common.Contract;
import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/** Connector 创建集中在边界层；地址、Topic、表名与凭据来源由配置决定。 */
public final class Connectors {
    private Connectors() {}

    public static MySqlSource<String> mysql(CommerceConfig config) {
        String database = config.get("mysql.database");
        if (!database.matches("[A-Za-z][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid database name");
        }
        return MySqlSource.<String>builder()
                .hostname(config.get("mysql.host"))
                .port(Math.toIntExact(config.positive("mysql.port")))
                .databaseList(database)
                .tableList(
                        Contract.CDC_TABLES.stream()
                                .map(table -> database + "." + table)
                                .toArray(String[]::new))
                .username(config.get("cdc.username"))
                .password(config.secret("COMMERCE_CDC_PASSWORD"))
                .serverId(config.get("cdc.server.ids"))
                .serverTimeZone("Asia/Shanghai")
                .startupOptions(StartupOptions.initial())
                .splitSize(Math.toIntExact(config.positive("cdc.snapshot.chunk.size")))
                .includeSchemaChanges(false)
                .deserializer(new JsonDebeziumDeserializationSchema(false))
                .build();
    }

    public static KafkaSource<String> kafka(
            CommerceConfig config, List<String> topics, String groupKey) {
        // 全新启动才使用 earliest；从 checkpoint 恢复时使用状态中记录的消费位点。
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.get("kafka.bootstrap.servers"))
                .setTopics(topics)
                .setGroupId(config.get(groupKey))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSink<String> kafkaSink(CommerceConfig config, String topic, String suffix) {
        return kafkaSink(
                config,
                KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build(),
                suffix);
    }

    public static KafkaSink<String> kafkaSink(
            CommerceConfig config,
            KafkaRecordSerializationSchema<String> serializer,
            String suffix) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.get("kafka.bootstrap.servers"))
                .setRecordSerializer(serializer)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(
                        config.get("kafka.transaction.prefix") + "-" + suffix + "-")
                .setProperty("transaction.timeout.ms", config.get("kafka.transaction.timeout.ms"))
                .setProperty("compression.type", "lz4")
                .setProperty("linger.ms", "20")
                .setProperty("batch.size", "65536")
                .build();
    }

    public static DorisSink<String> doris(CommerceConfig config, String tableKey, String suffix) {
        // 2PC 跟随 checkpoint 提交；严格模式不允许悄悄过滤导入失败的金额记录。
        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty("format", "json");
        streamLoadProperties.setProperty("read_json_by_line", "true");
        streamLoadProperties.setProperty("strict_mode", "true");
        streamLoadProperties.setProperty("max_filter_ratio", "0");
        return DorisSink.<String>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes(config.get("doris.fenodes"))
                                .setUsername(config.get("doris.username"))
                                .setPassword(config.secret("COMMERCE_DORIS_PASSWORD"))
                                .setTableIdentifier(config.get(tableKey))
                                .build())
                .setDorisReadOptions(DorisReadOptions.defaults())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .enable2PC()
                                .setDeletable(false)
                                .setLabelPrefix(config.get("doris.label.prefix") + "_" + suffix)
                                .setStreamLoadProp(streamLoadProperties)
                                .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

    /** DWD以订单ID作为Kafka key，同一订单的事实尽量保持分区内顺序。 */
    public static class TradeFactSerializer implements KafkaRecordSerializationSchema<String> {
        private final String topic;

        public TradeFactSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String raw, KafkaSinkContext context, Long timestamp) {
            String orderId = Json.text(Json.object(raw), "orderId");
            return new ProducerRecord<>(
                    topic,
                    orderId.getBytes(StandardCharsets.UTF_8),
                    raw.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class CdcRouter implements KafkaRecordSerializationSchema<String> {
        private final CommerceConfig config;

        public CdcRouter(CommerceConfig config) {
            this.config = config;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String raw, KafkaSinkContext context, Long timestamp) {
            JsonNode root = Json.object(raw);
            if (root.has("payload")) {
                root = root.path("payload");
            }
            String table = Json.text(root.path("source"), "table");
            if (!Contract.CDC_TABLES.contains(table)) {
                throw new IllegalArgumentException("Unexpected CDC table");
            }
            var row = root.path(Json.text(root, "op").equals("d") ? "before" : "after");
            String key = Json.text(row, "trade_outbox".equals(table) ? "order_id" : "id");
            return new ProducerRecord<>(
                    config.topic(table),
                    key.getBytes(StandardCharsets.UTF_8),
                    raw.getBytes(StandardCharsets.UTF_8));
        }
    }
}
