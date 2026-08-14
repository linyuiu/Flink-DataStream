package org.linyu.sourcedata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.linyu.config.BusinessTime;
import org.linyu.config.ConfigUtil;

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

    private static final long INTERVAL_MS = 1000L;
    private static final int USER_COUNT = 200;
    private static final int SKU_COUNT = 50;
    private static final int KEEP_UNPAID_RATE = 10;
    private static final int CANCEL_BEFORE_PAYMENT_RATE = 10;
    private static final int PARTIAL_REFUND_RATE = 15;
    private static final int FULL_REFUND_RATE = 10;
    private static final int PARTIAL_TO_FULL_REFUND_RATE = 20;
    private static final BigDecimal ZERO_AMOUNT =
            BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        MockDataProducerSource source = new MockDataProducerSource(
                ConfigUtil.getLong("mock.data.order.interval.ms",100L),
                USER_COUNT,
                SKU_COUNT,
                ConfigUtil.getString("kafka.bootstrap.servers"),
                ConfigUtil.getString("kafka.topic.gvm_realtime_produce"),
                ConfigUtil.getString("kafka.topic.user.active_produce"),
                ConfigUtil.getString("kafka.topic.dim.user_produce")
        );

        env.addSource(source)
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
        private final String bootstrapServers;
        private final String orderDetailTopic;
        private final String userActiveTopic;
        private final String dimUserTopic;

        private transient KafkaProducer<String, String> producer;
        private long orderSeq = 1L;
        private int currentActiveUserIndex = 1;
        private int currentDimUserIndex = 1;
        private LocalDate activeLogicalDate =
                LocalDate.now(BusinessTime.ZONE_ID);
        private LocalDate dimLogicalDate =
                LocalDate.now(BusinessTime.ZONE_ID);

        private MockDataProducerSource(
                long intervalMs,
                int userCount,
                int skuCount,
                String bootstrapServers,
                String orderDetailTopic,
                String userActiveTopic,
                String dimUserTopic
        ) {
            super(intervalMs, userCount);
            this.skuCount = skuCount;
            this.bootstrapServers = bootstrapServers;
            this.orderDetailTopic = orderDetailTopic;
            this.userActiveTopic = userActiveTopic;
            this.dimUserTopic = dimUserTopic;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("key.serializer", StringSerializer.class.getName());
            props.put("value.serializer", StringSerializer.class.getName());
            props.put("acks", "all");
            props.put("enable.idempotence", "true");
            props.put("retries", Integer.toString(Integer.MAX_VALUE));
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
                    LocalDateTime.now(BusinessTime.ZONE_ID)
                            .minusMinutes(2 + random.nextInt(119));
            BigDecimal orderAmount = randomAmount(20, 500);

            String orderId = "O" + System.currentTimeMillis() + "_" + orderSeq++;

            /*
             * 每条消息都是订单当前时刻的全量快照：
             * 同一个订单始终使用相同 Kafka key，保证进入同一分区并保持状态顺序。
             */
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("order_id", orderId);
            row.put("user_id", userId(1 + random.nextInt(userCount)));
            row.put("sku_id", String.valueOf(1 + random.nextInt(skuCount)));
            row.put("order_amount", orderAmount);
            row.put("pay_amount", ZERO_AMOUNT);
            row.put("refund_amount", ZERO_AMOUNT);
            row.put("order_status", "UNPAID");
            row.put("create_time", format(createTime));
            row.put("pay_time", "");
            row.put("update_time", format(createTime));
            row.put("dt", dt(createTime));

            send(orderDetailTopic, orderId, row);

            int paymentOutcome = random.nextInt(100);

            // 一部分订单保留在待支付状态，模拟真实在途订单。
            if (paymentOutcome < KEEP_UNPAID_RATE) {
                return;
            }

            LocalDateTime firstUpdateTime =
                    createTime.plusSeconds(1 + random.nextInt(30));

            // 未支付订单只能取消，不应伪装成退款订单。
            if (paymentOutcome
                    < KEEP_UNPAID_RATE + CANCEL_BEFORE_PAYMENT_RATE) {
                sendCancelledSnapshot(orderId, row, firstUpdateTime);
                return;
            }

            LocalDateTime payTime = firstUpdateTime;
            sendPaidSnapshot(orderId, row, orderAmount, payTime);

            int refundOutcome = random.nextInt(100);
            if (refundOutcome < PARTIAL_REFUND_RATE) {
                BigDecimal partialRefund = randomPartialRefund(orderAmount);
                LocalDateTime refundTime =
                        payTime.plusSeconds(1 + random.nextInt(30));

                sendRefundSnapshot(
                        orderId,
                        row,
                        orderAmount,
                        partialRefund,
                        "PART_REFUNDED",
                        refundTime
                );

                if (random.nextInt(100) < PARTIAL_TO_FULL_REFUND_RATE) {
                    sendRefundSnapshot(
                            orderId,
                            row,
                            orderAmount,
                            orderAmount,
                            "REFUNDED",
                            refundTime.plusSeconds(1 + random.nextInt(30))
                    );
                }
            } else if (refundOutcome
                    < PARTIAL_REFUND_RATE + FULL_REFUND_RATE) {
                sendRefundSnapshot(
                        orderId,
                        row,
                        orderAmount,
                        orderAmount,
                        "REFUNDED",
                        payTime.plusSeconds(1 + random.nextInt(30))
                );
            }
        }

        private void sendCancelledSnapshot(
                String orderId,
                Map<String, Object> currentSnapshot,
                LocalDateTime updateTime
        ) throws Exception {
            Map<String, Object> cancelRow =
                    new LinkedHashMap<>(currentSnapshot);
            cancelRow.put("order_status", "CANCELLED");
            updateVersionFields(cancelRow, updateTime);

            send(orderDetailTopic, orderId, cancelRow);
            currentSnapshot.clear();
            currentSnapshot.putAll(cancelRow);
        }

        private void sendPaidSnapshot(
                String orderId,
                Map<String, Object> currentSnapshot,
                BigDecimal payAmount,
                LocalDateTime payTime
        ) throws Exception {
            Map<String, Object> paidRow =
                    new LinkedHashMap<>(currentSnapshot);
            paidRow.put("pay_amount", payAmount);
            paidRow.put("refund_amount", ZERO_AMOUNT);
            paidRow.put("order_status", "PAID");
            paidRow.put("pay_time", format(payTime));
            updateVersionFields(paidRow, payTime);

            send(orderDetailTopic, orderId, paidRow);
            currentSnapshot.clear();
            currentSnapshot.putAll(paidRow);
        }

        private void sendRefundSnapshot(
                String orderId,
                Map<String, Object> currentSnapshot,
                BigDecimal payAmount,
                BigDecimal cumulativeRefundAmount,
                String status,
                LocalDateTime updateTime
        ) throws Exception {
            Map<String, Object> refundRow =
                    new LinkedHashMap<>(currentSnapshot);
            refundRow.put("pay_amount", payAmount);
            refundRow.put("refund_amount", cumulativeRefundAmount);
            refundRow.put("order_status", status);
            updateVersionFields(refundRow, updateTime);

            send(orderDetailTopic, orderId, refundRow);
            currentSnapshot.clear();
            currentSnapshot.putAll(refundRow);
        }

        private void updateVersionFields(
                Map<String, Object> row,
                LocalDateTime updateTime
        ) {
            row.put("update_time", format(updateTime));
            row.put("dt", dt(updateTime));
        }

        private void sendUserActiveLog() throws Exception {
            LocalDateTime activeTime = randomTimeInDay(activeLogicalDate);

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("user_id", userId(currentActiveUserIndex));
            row.put("active_time", activeTime.format(DATE_TIME_FORMATTER));
            row.put("dt", activeLogicalDate.toString());

            send(userActiveTopic, row.get("user_id").toString(), row);

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

            send(dimUserTopic, row.get("user_id").toString(), row);

            currentDimUserIndex++;
            if (currentDimUserIndex > userCount) {
                currentDimUserIndex = 1;
                // 维度表按 dt 生成每日快照，便于和事实表按分区日期关联。
                dimLogicalDate = dimLogicalDate.plusDays(1);
            }
        }

        private void send(String topic, String key, Map<String, Object> row) throws Exception {
            // 等待 broker 确认，避免模拟器表面运行但消息异步发送失败。
            producer.send(
                    new ProducerRecord<>(topic, key, toJson(row))
            ).get();
        }

        private BigDecimal randomPartialRefund(BigDecimal payAmount) {
            int payAmountInCents =
                    payAmount.movePointRight(2).intValueExact();
            int refundInCents =
                    1 + random.nextInt(payAmountInCents - 1);

            return BigDecimal.valueOf(refundInCents, 2)
                    .setScale(2, RoundingMode.HALF_UP);
        }

        private BigDecimal randomAmount(int min, int max) {
            double amount = min + random.nextDouble() * (max - min);
            return BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP);
        }

        private String format(LocalDateTime dateTime) {
            return dateTime.format(DATE_TIME_FORMATTER);
        }
    }

}
