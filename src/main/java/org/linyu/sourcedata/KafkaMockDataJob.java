package org.linyu.sourcedata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * 向 Kafka 写入 DWD/维度模拟数据。
 */
public class KafkaMockDataJob {

    private static final String BROKERS = "localhost:9092";
    private static final long INTERVAL_MS = 1000L;
    private static final int USER_COUNT = 200;
    private static final int SKU_COUNT = 50;

    private static final String ORDER_DETAIL_TOPIC = "dwd_order_detail";
    private static final String USER_ACTIVE_TOPIC = "dwd_user_active_log";
    private static final String DIM_USER_TOPIC = "dim_user";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new OrderDetailSource(INTERVAL_MS, USER_COUNT, SKU_COUNT))
                .name("mock-order-detail-source")
                .sinkTo(buildKafkaSink(BROKERS, ORDER_DETAIL_TOPIC))
                .name("mock-order-detail-kafka-sink");

        env.addSource(new UserActiveLogSource(INTERVAL_MS, USER_COUNT))
                .name("mock-user-active-log-source")
                .sinkTo(buildKafkaSink(BROKERS, USER_ACTIVE_TOPIC))
                .name("mock-user-active-log-kafka-sink");

        env.addSource(new DimUserSource(INTERVAL_MS, USER_COUNT))
                .name("mock-dim-user-source")
                .sinkTo(buildKafkaSink(BROKERS, DIM_USER_TOPIC))
                .name("mock-dim-user-kafka-sink");

        env.execute("Kafka Mock Data Job");
    }

    private static KafkaSink<String> buildKafkaSink(String brokers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 模拟数据允许少量重复，使用 at-least-once 可以避免 exactly-once 事务配置的额外要求。
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private abstract static class BaseJsonSource extends RichParallelSourceFunction<String> {

        protected static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        protected final long intervalMs;
        protected final int userCount;
        protected final Random random = new Random();

        private transient ObjectMapper objectMapper;
        private volatile boolean running = true;

        protected BaseJsonSource(long intervalMs, int userCount) {
            this.intervalMs = intervalMs;
            this.userCount = userCount;
        }

        @Override
        public void cancel() {
            running = false;
        }

        protected boolean isRunning() {
            return running;
        }

        protected String toJson(Map<String, Object> row) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }
            return objectMapper.writeValueAsString(row);
        }

        protected String userId(int index) {
            return String.valueOf(index);
        }

        protected String dt(LocalDateTime time) {
            return time.toLocalDate().toString();
        }

        protected LocalDateTime randomTimeInDay(LocalDate date) {
            return LocalDateTime.of(
                    date,
                    LocalTime.of(
                            random.nextInt(24),
                            random.nextInt(60),
                            random.nextInt(60)
                    )
            );
        }

        protected void pause() throws InterruptedException {
            Thread.sleep(intervalMs);
        }
    }

    private static class OrderDetailSource extends BaseJsonSource {

        private final int skuCount;

        private long orderSeq = 1L;

        private OrderDetailSource(long intervalMs, int userCount, int skuCount) {
            super(intervalMs, userCount);
            this.skuCount = skuCount;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning()) {
                LocalDateTime createTime =
                        LocalDateTime.now().minusMinutes(random.nextInt(120));
                String orderStatus = randomOrderStatus();
                LocalDateTime payTime =
                        "CREATED".equals(orderStatus)
                                ? null
                                : createTime.plusMinutes(1 + random.nextInt(30));

                BigDecimal payAmount = randomAmount(20, 500);
                BigDecimal refundAmount =
                        "REFUNDED".equals(orderStatus)
                                ? payAmount
                                : BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("order_id", "O" + System.currentTimeMillis() + "_" + orderSeq++);
                row.put("user_id", userId(1 + random.nextInt(userCount)));
                row.put("sku_id", String.valueOf(1 + random.nextInt(skuCount)));
                row.put("pay_amount", payAmount);
                row.put("refund_amount", refundAmount);
                row.put("order_status", orderStatus);
                row.put("create_time", createTime.format(DATE_TIME_FORMATTER));
                row.put(
                        "pay_time",
                        payTime == null ? "" : payTime.format(DATE_TIME_FORMATTER)
                );
                row.put("dt", dt(createTime));

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(toJson(row));
                }

                pause();
            }
        }

        private String randomOrderStatus() {
            int value = random.nextInt(100);
            if (value < 10) {
                return "CREATED";
            }
            if (value < 75) {
                return "PAID";
            }
            if (value < 95) {
                return "FINISHED";
            }
            return "REFUNDED";
        }

        private BigDecimal randomAmount(int min, int max) {
            double amount = min + random.nextDouble() * (max - min);
            return BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP);
        }
    }

    private static class UserActiveLogSource extends BaseJsonSource {

        private int currentUserIndex = 1;
        private LocalDate logicalDate = LocalDate.now();

        private UserActiveLogSource(long intervalMs, int userCount) {
            super(intervalMs, userCount);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning()) {
                LocalDateTime activeTime = randomTimeInDay(logicalDate);

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("user_id", userId(currentUserIndex));
                row.put("active_time", activeTime.format(DATE_TIME_FORMATTER));
                row.put("dt", logicalDate.toString());

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(toJson(row));
                }

                nextUserAndDate();
                pause();
            }
        }

        private void nextUserAndDate() {
            currentUserIndex++;
            if (currentUserIndex > userCount) {
                currentUserIndex = 1;
                // 保证活跃日志满足“一天一用户一条”，完整输出一天用户后再推进到下一天。
                logicalDate = logicalDate.plusDays(1);
            }
        }
    }

    private static class DimUserSource extends BaseJsonSource {

        private int currentUserIndex = 1;
        private LocalDate logicalDate = LocalDate.now();

        private DimUserSource(long intervalMs, int userCount) {
            super(intervalMs, userCount);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning()) {
                LocalDateTime registerTime =
                        randomTimeInDay(logicalDate.minusDays(1 + random.nextInt(365)));

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("user_id", userId(currentUserIndex));
                row.put("register_time", registerTime.format(DATE_TIME_FORMATTER));
                row.put("dt", logicalDate.toString());

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(toJson(row));
                }

                nextUserAndDate();
                pause();
            }
        }

        private void nextUserAndDate() {
            currentUserIndex++;
            if (currentUserIndex > userCount) {
                currentUserIndex = 1;
                // 维度表按 dt 生成每日快照，便于和事实表按分区日期关联。
                logicalDate = logicalDate.plusDays(1);
            }
        }
    }
}
