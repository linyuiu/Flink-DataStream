package org.linyu.sourcedata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Properties;
import java.util.Random;

/**
 * 向 Kafka 写入 DWD/维度模拟数据。
 */
public class KafkaMockDataJob {

    private static final String BROKERS = "kafka:9092";
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

        env.addSource(new MockDataProducerSource(INTERVAL_MS, USER_COUNT, SKU_COUNT))
                .name("mock-data-producer-source")
                .print()
                .name("mock-data-progress-print");

        env.execute("Kafka Mock Data Job");
    }

    private abstract static class BaseJsonSource extends RichParallelSourceFunction<String> {

        protected static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        protected final long intervalMs;
        protected final int userCount;
        protected final Random random = new Random();

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

        protected String toJson(Map<String, Object> row) {
            StringBuilder builder = new StringBuilder();
            builder.append('{');

            boolean first = true;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!first) {
                    builder.append(',');
                }
                first = false;

                builder.append('"')
                        .append(escapeJson(entry.getKey()))
                        .append("\":");
                appendJsonValue(builder, entry.getValue());
            }

            builder.append('}');
            return builder.toString();
        }

        private void appendJsonValue(StringBuilder builder, Object value) {
            if (value == null) {
                builder.append("null");
                return;
            }

            if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
                return;
            }

            builder.append('"')
                    .append(escapeJson(value.toString()))
                    .append('"');
        }

        private String escapeJson(String value) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '"':
                        builder.append("\\\"");
                        break;
                    case '\\':
                        builder.append("\\\\");
                        break;
                    case '\b':
                        builder.append("\\b");
                        break;
                    case '\f':
                        builder.append("\\f");
                        break;
                    case '\n':
                        builder.append("\\n");
                        break;
                    case '\r':
                        builder.append("\\r");
                        break;
                    case '\t':
                        builder.append("\\t");
                        break;
                    default:
                        if (c < 0x20) {
                            builder.append(String.format("\\u%04x", (int) c));
                        } else {
                            builder.append(c);
                        }
                }
            }
            return builder.toString();
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

    private static class MockDataProducerSource extends BaseJsonSource {

        private final int skuCount;

        private transient KafkaProducer<String, String> producer;
        private long orderSeq = 1L;
        private int currentActiveUserIndex = 1;
        private int currentDimUserIndex = 1;
        private LocalDate activeLogicalDate = LocalDate.now();
        private LocalDate dimLogicalDate = LocalDate.now();

        private MockDataProducerSource(long intervalMs, int userCount, int skuCount) {
            super(intervalMs, userCount);
            this.skuCount = skuCount;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            Properties props = new Properties();
            props.put("bootstrap.servers", BROKERS);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            props.put("acks", "1");
            props.put("retries", "3");
            producer = new KafkaProducer<>(props);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long count = 0L;

            while (isRunning()) {
                sendOrderDetail();
                sendUserActiveLog();
                sendDimUser();

                count++;
                if (count % 10 == 0) {
                    ctx.collect("mock data sent: " + count + " batches");
                }

                pause();
            }
        }

        @Override
        public void close() {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        }

        private void sendOrderDetail() throws Exception {
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

            send(ORDER_DETAIL_TOPIC, row.get("order_id").toString(), row);
        }

        private void sendUserActiveLog() throws Exception {
            LocalDateTime activeTime = randomTimeInDay(activeLogicalDate);

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("user_id", userId(currentActiveUserIndex));
            row.put("active_time", activeTime.format(DATE_TIME_FORMATTER));
            row.put("dt", activeLogicalDate.toString());

            send(USER_ACTIVE_TOPIC, row.get("user_id").toString(), row);

            currentActiveUserIndex++;
            if (currentActiveUserIndex > userCount) {
                currentActiveUserIndex = 1;
                // 保证活跃日志满足“一天一用户一条”，完整输出一天用户后再推进到下一天。
                activeLogicalDate = activeLogicalDate.plusDays(1);
            }
        }

        private void sendDimUser() throws Exception {
            LocalDateTime registerTime =
                    randomTimeInDay(dimLogicalDate.minusDays(1 + random.nextInt(365)));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("user_id", userId(currentDimUserIndex));
            row.put("register_time", registerTime.format(DATE_TIME_FORMATTER));
            row.put("dt", dimLogicalDate.toString());

            send(DIM_USER_TOPIC, row.get("user_id").toString(), row);

            currentDimUserIndex++;
            if (currentDimUserIndex > userCount) {
                currentDimUserIndex = 1;
                // 维度表按 dt 生成每日快照，便于和事实表按分区日期关联。
                dimLogicalDate = dimLogicalDate.plusDays(1);
            }
        }

        private void send(String topic, String key, Map<String, Object> row) throws Exception {
            producer.send(new ProducerRecord<>(topic, key, toJson(row)));
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

}
