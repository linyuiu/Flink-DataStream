package org.inyu.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 订单 10 分钟未支付实时规则作业。
 *
 * 生产链路里优先把“未支付超时”作为业务事件写回 Kafka，后续短信、关单、落库等系统各自消费。
 */
public class UnpaidOrderTimeoutJob {

    private static final String BROKERS = "kafka:9092";
    private static final String SOURCE_TOPIC = "dwd_order_detail";
    private static final String RESULT_TOPIC = "dws_order_unpaid_timeout";
    private static final String GROUP_ID = "unpaid-order-timeout-rule-job";

    private static final String RULE_CODE = "UNPAID_ORDER_10MIN";
    private static final long UNPAID_TIMEOUT_MS = 1 * 60 * 1000L;
    private static final long KAFKA_TRANSACTION_TIMEOUT_MS = 10 * 60 * 1000L;
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 生产中 Kafka 结果流建议配合 checkpoint；下游再用 event_id 做幂等防重。
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(2 * 60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers(BROKERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<OrderEvent> orderStream = env
                .fromSource(
                        orderSource,
                        WatermarkStrategy.noWatermarks(),
                        "dwd-order-detail-kafka-source"
                )
                .map(new OrderEventParser())
                .filter(event -> event != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                                .withTimestampAssigner((event, timestamp) -> event.eventTimeMs())
                                .withIdleness(Duration.ofMinutes(1))
                );

        KafkaSink<String> timeoutEventSink = KafkaSink.<String>builder()
                .setBootstrapServers(BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(RESULT_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("unpaid-order-timeout-")
                // 必须小于 broker 的 transaction.max.timeout.ms；Kafka 默认最大通常是 15 分钟。
                .setProperty(
                        "transaction.timeout.ms",
                        String.valueOf(KAFKA_TRANSACTION_TIMEOUT_MS)
                )
                .build();

        orderStream
                .keyBy(event -> event.orderId)
                .process(new UnpaidOrderTimeoutFunction())
                .name("unpaid-order-timeout-function")
                .map(new TimeoutEventSerializer())
                .sinkTo(timeoutEventSink)
                .name("unpaid-order-timeout-kafka-sink");

        env.execute("Unpaid Order Timeout Job");
    }

    public static class OrderEvent implements Serializable {
        public String orderId;
        public String userId;
        public String skuId;
        public BigDecimal payAmount;
        public BigDecimal refundAmount;
        public String orderStatus;
        public String createTime;
        public String payTime;
        public String dt;
        public long createTimeMs;
        public long payTimeMs;

        public boolean isPaid() {
            return hasText(payTime)
                    || "PAID".equals(orderStatus)
                    || "FINISHED".equals(orderStatus)
                    || "REFUNDED".equals(orderStatus);
        }

        public long eventTimeMs() {
            return payTimeMs > 0 ? payTimeMs : createTimeMs;
        }
    }

    public static class UnpaidOrderTimeoutEvent implements Serializable {
        public String timeoutEventId;
        public String orderId;
        public String userId;
        public String skuId;
        public BigDecimal payAmount;
        public String orderStatus;
        public String ruleCode;
        public String createTime;
        public String timeoutTime;
        public String dt;
        public long createTimeMs;
        public long timeoutTimeMs;
    }

    public static class PendingOrderState implements Serializable {
        public String orderId;
        public String userId;
        public String skuId;
        public BigDecimal payAmount;
        public String orderStatus;
        public String createTime;
        public String dt;
        public long createTimeMs;
        public long timerTimeMs;

        public PendingOrderState() {
        }

        public PendingOrderState(OrderEvent event, long timerTimeMs) {
            this.orderId = event.orderId;
            this.userId = event.userId;
            this.skuId = event.skuId;
            this.payAmount = event.payAmount;
            this.orderStatus = event.orderStatus;
            this.createTime = event.createTime;
            this.dt = event.dt;
            this.createTimeMs = event.createTimeMs;
            this.timerTimeMs = timerTimeMs;
        }
    }

    public static class OrderEventParser implements MapFunction<String, OrderEvent> {

        private transient ObjectMapper mapper;

        @Override
        public OrderEvent map(String value) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }

            JsonNode root = mapper.readTree(value);
            String orderId = text(root, "order_id");
            String createTime = text(root, "create_time");

            if (!hasText(orderId) || !hasText(createTime)) {
                return null;
            }

            OrderEvent event = new OrderEvent();
            event.orderId = orderId;
            event.userId = text(root, "user_id");
            event.skuId = text(root, "sku_id");
            event.payAmount = decimal(root, "pay_amount");
            event.refundAmount = decimal(root, "refund_amount");
            event.orderStatus = text(root, "order_status");
            event.createTime = createTime;
            event.payTime = text(root, "pay_time");
            event.dt = text(root, "dt");
            event.createTimeMs = parseDateTime(createTime);
            event.payTimeMs = hasText(event.payTime) ? parseDateTime(event.payTime) : 0L;
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
            return hasText(value) ? new BigDecimal(value) : BigDecimal.ZERO;
        }
    }

    public static class UnpaidOrderTimeoutFunction
            extends KeyedProcessFunction<String, OrderEvent, UnpaidOrderTimeoutEvent> {

        private transient ValueState<PendingOrderState> pendingOrderState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<PendingOrderState> descriptor =
                    new ValueStateDescriptor<>(
                            "pending-order-state",
                            PendingOrderState.class
                    );
            descriptor.enableTimeToLive(ttl);
            pendingOrderState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                OrderEvent event,
                Context ctx,
                Collector<UnpaidOrderTimeoutEvent> out
        ) throws Exception {

            if (event.isPaid()) {
                // 支付、完成、退款都说明订单已不需要超时提醒，取消同订单等待中的 timer。
                PendingOrderState oldState = pendingOrderState.value();
                if (oldState != null) {
                    ctx.timerService().deleteEventTimeTimer(oldState.timerTimeMs);
                    pendingOrderState.clear();
                }
                return;
            }

            long timerTimeMs = event.createTimeMs + UNPAID_TIMEOUT_MS;
            PendingOrderState oldState = pendingOrderState.value();
            if (oldState != null) {
                ctx.timerService().deleteEventTimeTimer(oldState.timerTimeMs);
            }

            pendingOrderState.update(new PendingOrderState(event, timerTimeMs));
            ctx.timerService().registerEventTimeTimer(timerTimeMs);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<UnpaidOrderTimeoutEvent> out
        ) throws Exception {
            PendingOrderState state = pendingOrderState.value();
            if (state == null || timestamp != state.timerTimeMs) {
                return;
            }

            UnpaidOrderTimeoutEvent event = new UnpaidOrderTimeoutEvent();
            event.timeoutEventId = state.orderId + "_" + RULE_CODE;
            event.orderId = state.orderId;
            event.userId = state.userId;
            event.skuId = state.skuId;
            event.payAmount = state.payAmount;
            event.orderStatus = state.orderStatus;
            event.ruleCode = RULE_CODE;
            event.createTime = state.createTime;
            event.timeoutTime = formatDateTime(timestamp);
            event.dt = state.dt;
            event.createTimeMs = state.createTimeMs;
            event.timeoutTimeMs = timestamp;

            out.collect(event);
            pendingOrderState.clear();
        }
    }

    public static class TimeoutEventSerializer
            implements MapFunction<UnpaidOrderTimeoutEvent, String> {

        private transient ObjectMapper mapper;

        @Override
        public String map(UnpaidOrderTimeoutEvent value) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.writeValueAsString(value);
        }
    }

    public static long parseDateTime(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, DATE_TIME_FORMATTER);
        return localDateTime
                .atZone(BUSINESS_ZONE)
                .toInstant()
                .toEpochMilli();
    }

    public static String formatDateTime(long epochMillis) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMillis),
                BUSINESS_ZONE
        );
        return localDateTime.format(DATE_TIME_FORMATTER);
    }

    public static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
