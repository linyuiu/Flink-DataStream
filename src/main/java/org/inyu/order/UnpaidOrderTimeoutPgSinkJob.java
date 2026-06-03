package org.inyu.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * 未支付超时事件落 PostgreSQL 作业。
 *
 * 生产中建议把落库从主规则作业拆出来：Kafka 事件流负责分发，PG 只承接查询/审计场景。
 */
public class UnpaidOrderTimeoutPgSinkJob {

    private static final String BROKERS = "kafka:9092";
    private static final String SOURCE_TOPIC = "dws_order_unpaid_timeout";
    private static final String GROUP_ID = "unpaid-order-timeout-pg-sink-job";

    private static final String PG_URL = "jdbc:postgresql://postgres:5432/flink_realtime";
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "postgres";
    private static final String PG_TABLE = "public.unpaid_order_timeout";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // PG sink 使用 timeout_event_id 主键 upsert，即使 Kafka at-least-once 重放也不会重复插入。
        env.enableCheckpointing(60_000L, CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BROKERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(
                        source,
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "unpaid-timeout-kafka-source"
                )
                .map(new TimeoutEventParser())
                .filter(event -> event != null)
                .addSink(new PgUnpaidOrderSink())
                .name("pgsql-unpaid-order-sink");

        env.execute("Unpaid Order Timeout PG Sink Job");
    }

    public static class TimeoutEventParser
            implements MapFunction<String, UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent> {

        private transient ObjectMapper mapper;

        @Override
        public UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent map(String value) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            JsonNode root = mapper.readTree(value);
            String timeoutEventId = text(root, "timeoutEventId");
            String orderId = text(root, "orderId");

            if (!UnpaidOrderTimeoutJob.hasText(timeoutEventId)
                    || !UnpaidOrderTimeoutJob.hasText(orderId)) {
                return null;
            }

            UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent event =
                    new UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent();
            event.timeoutEventId = timeoutEventId;
            event.orderId = orderId;
            event.userId = text(root, "userId");
            event.skuId = text(root, "skuId");
            event.payAmount = decimal(root, "payAmount");
            event.orderStatus = text(root, "orderStatus");
            event.ruleCode = text(root, "ruleCode");
            event.createTime = text(root, "createTime");
            event.timeoutTime = text(root, "timeoutTime");
            event.dt = text(root, "dt");
            event.createTimeMs = root.path("createTimeMs").asLong(0L);
            event.timeoutTimeMs = root.path("timeoutTimeMs").asLong(0L);
            return event;
        }

        private static String text(JsonNode root, String field) {
            JsonNode node = root.path(field);
            if (node.isMissingNode() || node.isNull()) {
                return "";
            }
            return node.asText("");
        }

        private static BigDecimal decimal(JsonNode root, String field) {
            String value = text(root, field);
            return UnpaidOrderTimeoutJob.hasText(value)
                    ? new BigDecimal(value)
                    : BigDecimal.ZERO;
        }
    }

    public static class PgUnpaidOrderSink
            extends RichSinkFunction<UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent> {

        private static final String CREATE_TABLE_SQL =
                "CREATE TABLE IF NOT EXISTS " + PG_TABLE + " ("
                        + "timeout_event_id VARCHAR(128) PRIMARY KEY,"
                        + "order_id VARCHAR(64) NOT NULL,"
                        + "user_id VARCHAR(64) NOT NULL,"
                        + "sku_id VARCHAR(64),"
                        + "pay_amount NUMERIC(18,2),"
                        + "order_status VARCHAR(32),"
                        + "rule_code VARCHAR(64),"
                        + "create_time TIMESTAMP NOT NULL,"
                        + "timeout_time TIMESTAMP NOT NULL,"
                        + "dt VARCHAR(16),"
                        + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
                        + ")";

        private static final String UPSERT_SQL =
                "INSERT INTO " + PG_TABLE + " ("
                        + "timeout_event_id, order_id, user_id, sku_id, pay_amount,"
                        + "order_status, rule_code, create_time, timeout_time, dt, updated_at"
                        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) "
                        + "ON CONFLICT (timeout_event_id) DO UPDATE SET "
                        + "order_id = EXCLUDED.order_id,"
                        + "user_id = EXCLUDED.user_id,"
                        + "sku_id = EXCLUDED.sku_id,"
                        + "pay_amount = EXCLUDED.pay_amount,"
                        + "order_status = EXCLUDED.order_status,"
                        + "rule_code = EXCLUDED.rule_code,"
                        + "create_time = EXCLUDED.create_time,"
                        + "timeout_time = EXCLUDED.timeout_time,"
                        + "dt = EXCLUDED.dt,"
                        + "updated_at = CURRENT_TIMESTAMP";

        private transient Connection connection;
        private transient PreparedStatement upsertStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD);
            try (PreparedStatement statement = connection.prepareStatement(CREATE_TABLE_SQL)) {
                statement.executeUpdate();
            }
            upsertStatement = connection.prepareStatement(UPSERT_SQL);
        }

        @Override
        public void invoke(
                UnpaidOrderTimeoutJob.UnpaidOrderTimeoutEvent value,
                Context context
        ) throws Exception {
            upsertStatement.setString(1, value.timeoutEventId);
            upsertStatement.setString(2, value.orderId);
            upsertStatement.setString(3, value.userId);
            upsertStatement.setString(4, value.skuId);
            upsertStatement.setBigDecimal(5, value.payAmount);
            upsertStatement.setString(6, value.orderStatus);
            upsertStatement.setString(7, value.ruleCode);
            upsertStatement.setTimestamp(8, Timestamp.valueOf(value.createTime));
            upsertStatement.setTimestamp(9, Timestamp.valueOf(value.timeoutTime));
            upsertStatement.setString(10, value.dt);
            upsertStatement.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (upsertStatement != null) {
                upsertStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
