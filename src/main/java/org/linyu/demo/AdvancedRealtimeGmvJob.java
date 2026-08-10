package org.linyu.demo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/**
 * 复杂版实时 GMV。
 *
 * 能够处理：
 * 1. 同一订单重复发送；
 * 2. PAID -> PART_REFUNDED；
 * 3. PAID -> REFUNDED；
 * 4. PAID -> CANCELLED；
 * 5. 金额被更新；
 * 6. 每5秒向Doris输出一次最新GMV。
 */
public class AdvancedRealtimeGmvJob {

    private static final String BOOTSTRAP_SERVERS =
            "192.168.9.53:9092";

    private static final String KAFKA_TOPIC =
            "dwd_order_detail";

    private static final String KAFKA_GROUP_ID =
            "advanced-realtime-gmv-job";

    private static final String DORIS_FE_NODES =
            "192.168.9.53:8030";

    private static final String DORIS_TABLE =
            "realtime_ads.gmv_realtime_today";

    private static final String DORIS_USERNAME =
            "root";

    private static final String DORIS_PASSWORD =
            "";

    /**
     * 是否从 GMV 中扣除退款。
     *
     * true：
     * 净GMV = pay_amount - refund_amount
     *
     * false：
     * 毛GMV = pay_amount
     */
    private static final boolean DEDUCT_REFUNDS = true;

    private static final long OUTPUT_INTERVAL_MS =
            5_000L;

    private static final ZoneId BUSINESS_ZONE =
            ZoneId.of("Asia/Shanghai");

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );

    /**
     * JSON解析失败的脏数据。
     */
    private static final OutputTag<String> JSON_DIRTY_TAG =
            new OutputTag<String>("json-dirty-data") {
            };

    /**
     * 业务字段非法的数据。
     */
    private static final OutputTag<String> BUSINESS_DIRTY_TAG =
            new OutputTag<String>("business-dirty-data") {
            };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        configureCheckpoint(env);

        KafkaSource<String> kafkaSource =
                KafkaSource.<String>builder()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setTopics(KAFKA_TOPIC)
                        .setGroupId(KAFKA_GROUP_ID)
                        .setStartingOffsets(
                                OffsetsInitializer.committedOffsets(
                                        OffsetResetStrategy.EARLIEST
                                )
                        )
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema()
                        )
                        .build();

        /*
         * 1. 从 Kafka 读取 JSON 字符串。
         */
        DataStreamSource<String> kafkaJsonStream =
                env.fromSource(
                                kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Order Source"
                        );
                        kafkaJsonStream.uid("advanced-kafka-order-source");

        /*
         * 2. 解析 JSON。
         *
         * 成功：
         * 输出到主流 OrderDetail。
         *
         * 失败：
         * 输出到 JSON_DIRTY_TAG。
         */
        SingleOutputStreamOperator<OrderDetail> orderStream =
                kafkaJsonStream
                        .process(
                                new OrderJsonProcessFunction()
                        )
                        .name("parse-order-json")
                        .uid("advanced-parse-order-json");

        DataStream<String> jsonDirtyStream =
                orderStream.getSideOutput(
                        JSON_DIRTY_TAG
                );

        /*
         * 3. 按照 order_id 分组。
         *
         * 相同 order_id 的所有状态更新，
         * 必须进入同一个 KeyedProcessFunction 实例。
         */
        SingleOutputStreamOperator<GmvDelta> deltaStream =
                orderStream
                        .keyBy(order -> order.orderId)

                        /*
                         * 保存订单上一次状态，
                         * 计算本次状态相对于上一次状态的变化量。
                         */
                        .process(
                                new OrderContributionProcessFunction()
                        )
                        .name("calculate-order-gmv-delta")
                        .uid("calculate-order-gmv-delta");

        DataStream<String> businessDirtyStream =
                deltaStream.getSideOutput(
                        BUSINESS_DIRTY_TAG
                );

        /*
         * 4. 按业务日期累计变化量。
         *
         * 输入示例：
         *
         * 2026-07-31 +100
         * 2026-07-31  -30
         * 2026-07-31  -70
         *
         * 最终：
         * 2026-07-31 = 0
         */
        SingleOutputStreamOperator<DailyGmv> dailyGmvStream =
                deltaStream
                        .keyBy(delta -> delta.bizDate)
                        .process(
                                new DailyGmvAccumulatorFunction()
                        )
                        .name("accumulate-daily-gmv")
                        .uid("accumulate-daily-gmv");

        /*
         * 5. 转换成 Doris JSON。
         */
        SingleOutputStreamOperator<String> dorisJsonStream =
                dailyGmvStream
                        .process(
                                new DailyGmvJsonProcessFunction()
                        )
                        .name("daily-gmv-to-json")
                        .uid("daily-gmv-to-json");

        /*
         * 6. 写入 Doris。
         */
        dorisJsonStream
                .sinkTo(buildDorisSink())
                .name("advanced-doris-gmv-sink")
                .uid("advanced-doris-gmv-sink");

        /*
         * 这里只是演示。
         *
         * 正式环境应该把脏数据写入：
         * 1. Kafka dirty topic；
         * 2. Doris脏数据表；
         * 3. 日志系统。
         */
        jsonDirtyStream
                .union(businessDirtyStream)
                .print("DIRTY");

        env.execute("Advanced Realtime Daily GMV");
    }

    /**
     * 配置Checkpoint。
     */
    private static void configureCheckpoint(
            StreamExecutionEnvironment env) {

        env.enableCheckpointing(
                10_000L,
                CheckpointingMode.EXACTLY_ONCE
        );

        CheckpointConfig checkpointConfig =
                env.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(
                60_000L
        );

        checkpointConfig.setMinPauseBetweenCheckpoints(
                5_000L
        );

        checkpointConfig.setMaxConcurrentCheckpoints(
                1
        );

        checkpointConfig.setTolerableCheckpointFailureNumber(
                3
        );

        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup
                        .RETAIN_ON_CANCELLATION
        );

        String checkpointDirectory =
                System.getProperty(
                        "checkpoint.dir",
                        "file:///tmp/flink-checkpoints/advanced-gmv"
                );

        Configuration configuration =
                new Configuration();

        configuration.set(
                CheckpointingOptions.CHECKPOINT_STORAGE,
                "filesystem"
        );

        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                checkpointDirectory
        );

        env.configure(configuration);
    }

    /**
     * JSON解析算子。
     */
    public static class OrderJsonProcessFunction
            extends ProcessFunction<String, OrderDetail> {

        private transient ObjectMapper objectMapper;

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void processElement(
                String json,
                Context context,
                Collector<OrderDetail> out) {

            try {
                OrderDetail order =
                        objectMapper.readValue(
                                json,
                                OrderDetail.class
                        );

                if (isBlank(order.orderId)) {
                    throw new IllegalArgumentException(
                            "order_id不能为空"
                    );
                }

                if (isBlank(order.orderStatus)) {
                    throw new IllegalArgumentException(
                            "order_status不能为空"
                    );
                }

                out.collect(order);

            } catch (Exception exception) {

                context.output(
                        JSON_DIRTY_TAG,
                        "原始数据：" + json
                                + "，错误原因："
                                + exception.getMessage()
                );
            }
        }
    }

    /**
     * 订单贡献变化计算。
     *
     * Key：
     * order_id
     *
     * State：
     * 该订单上一次对GMV的贡献。
     */
    public static class OrderContributionProcessFunction
            extends KeyedProcessFunction<
            String,
            OrderDetail,
            GmvDelta> {

        private transient ValueState<OrderContributionState>
                previousContributionState;

        @Override
        public void open(OpenContext openContext) {

            ValueStateDescriptor<OrderContributionState>
                    stateDescriptor =
                    new ValueStateDescriptor<>(
                            "order-contribution-state",
                            OrderContributionState.class
                    );

            previousContributionState =
                    getRuntimeContext().getState(
                            stateDescriptor
                    );
        }

        @Override
        public void processElement(
                OrderDetail order,
                Context context,
                Collector<GmvDelta> out) throws Exception {

            OrderContributionState previousState =
                    previousContributionState.value();

            /*
             * update_time 是推荐上游补充的字段。
             *
             * 有 update_time：
             * 可以识别迟到的旧版本订单。
             *
             * 没有 update_time：
             * 只能假设 Kafka 中同一 order_id
             * 按正确业务顺序到达。
             */
            Long currentVersion;

            try {
                currentVersion =
                        parseVersion(order.updateTime);
            } catch (Exception exception) {

                context.output(
                        BUSINESS_DIRTY_TAG,
                        "order_id=" + order.orderId
                                + "，update_time格式错误："
                                + order.updateTime
                );

                return;
            }

            /*
             * 已经处理过有版本的数据，
             * 后续却收到无版本数据。
             *
             * 为防止旧数据覆盖新状态，这里拒绝处理。
             */
            if (previousState != null
                    && previousState.lastVersion != null
                    && currentVersion == null) {

                context.output(
                        BUSINESS_DIRTY_TAG,
                        "order_id=" + order.orderId
                                + "，历史数据有版本号，"
                                + "当前消息缺少update_time"
                );

                return;
            }

            /*
             * 当前版本早于或等于已经处理过的版本，
             * 认为是旧数据或重复数据，直接忽略。
             */
            if (previousState != null
                    && previousState.lastVersion != null
                    && currentVersion != null
                    && currentVersion
                    <= previousState.lastVersion) {

                return;
            }

            BigDecimal newContribution =
                    calculateContribution(order);

            if (newContribution == null) {

                context.output(
                        BUSINESS_DIRTY_TAG,
                        "order_id=" + order.orderId
                                + "，无法识别的订单状态："
                                + order.orderStatus
                );

                return;
            }

            BigDecimal oldContribution =
                    previousState == null
                            ? BigDecimal.ZERO
                            : zeroIfNull(
                            previousState.contribution
                    );

            String oldBizDate =
                    previousState == null
                            ? null
                            : previousState.bizDate;

            String newBizDate =
                    extractNullableDate(
                            order.payTime
                    );

            /*
             * 取消或退款事件可能没有pay_time。
             *
             * 已经存在历史状态时，
             * 使用原订单的业务日期。
             */
            if (newBizDate == null
                    && previousState != null) {

                newBizDate =
                        previousState.bizDate;
            }

            /*
             * 新贡献不为0，却没有业务日期，
             * 无法判断金额应该计入哪一天。
             */
            if (newContribution
                    .compareTo(BigDecimal.ZERO) != 0
                    && newBizDate == null) {

                context.output(
                        BUSINESS_DIRTY_TAG,
                        "order_id=" + order.orderId
                                + "，存在GMV贡献但pay_time为空"
                );

                return;
            }

            /*
             * 情况一：
             * 订单前后属于同一天。
             *
             * 只需要输出：
             * 新贡献 - 旧贡献
             */
            if (Objects.equals(
                    oldBizDate,
                    newBizDate)) {

                BigDecimal delta =
                        newContribution.subtract(
                                oldContribution
                        );

                emitDelta(
                        newBizDate,
                        delta,
                        order.orderId,
                        out
                );

            } else {

                /*
                 * 情况二：
                 * 订单业务日期发生变化。
                 *
                 * 从旧日期减掉旧贡献。
                 */
                if (oldBizDate != null
                        && oldContribution.compareTo(
                        BigDecimal.ZERO) != 0) {

                    emitDelta(
                            oldBizDate,
                            oldContribution.negate(),
                            order.orderId,
                            out
                    );
                }

                /*
                 * 给新日期增加新贡献。
                 */
                if (newBizDate != null
                        && newContribution.compareTo(
                        BigDecimal.ZERO) != 0) {

                    emitDelta(
                            newBizDate,
                            newContribution,
                            order.orderId,
                            out
                    );
                }
            }

            Long nextVersion =
                    currentVersion != null
                            ? currentVersion
                            : previousState == null
                            ? null
                            : previousState.lastVersion;

            /*
             * 更新这个订单的最新状态。
             *
             * ValueState 会跟随Checkpoint保存。
             */
            previousContributionState.update(
                    new OrderContributionState(
                            newBizDate,
                            newContribution,
                            nextVersion
                    )
            );
        }

        private void emitDelta(
                String bizDate,
                BigDecimal delta,
                String orderId,
                Collector<GmvDelta> out) {

            if (bizDate == null
                    || delta == null
                    || delta.compareTo(
                    BigDecimal.ZERO) == 0) {

                return;
            }

            out.collect(
                    new GmvDelta(
                            bizDate,
                            delta,
                            orderId
                    )
            );
        }

        /**
         * 根据订单当前状态，
         * 计算这个订单现在应该对GMV贡献多少钱。
         */
        private BigDecimal calculateContribution(
                OrderDetail order) {

            String status =
                    order.orderStatus
                            .trim()
                            .toUpperCase(Locale.ROOT);

            BigDecimal payAmount =
                    zeroIfNull(order.payAmount);

            BigDecimal refundAmount =
                    zeroIfNull(order.refundAmount);

            switch (status) {

                /*
                 * 未支付、取消、关闭订单不产生GMV贡献。
                 */
                case "UNPAID":
                case "CANCELLED":
                case "CANCELED":
                case "CLOSED":
                    return BigDecimal.ZERO;

                /*
                 * 已支付和退款状态。
                 */
                case "PAID":
                case "PART_REFUNDED":
                case "PARTIALLY_REFUNDED":
                case "REFUNDED":

                    if (!DEDUCT_REFUNDS) {
                        return payAmount;
                    }

                    BigDecimal netAmount =
                            payAmount.subtract(
                                    refundAmount
                            );

                    /*
                     * 防止脏数据导致净金额小于0。
                     */
                    return netAmount.max(
                            BigDecimal.ZERO
                    );

                default:
                    return null;
            }
        }
    }

    /**
     * 按照日期累计GMV变化量。
     *
     * Key：
     * bizDate
     *
     * State：
     * 这一天当前GMV总额。
     *
     * Timer：
     * 每5秒最多输出一次当前结果。
     */
    public static class DailyGmvAccumulatorFunction
            extends KeyedProcessFunction<
            String,
            GmvDelta,
            DailyGmv> {

        private transient ValueState<BigDecimal>
                totalGmvState;

        private transient ValueState<Long>
                nextTimerState;

        private transient ValueState<Boolean>
                dirtyState;

        @Override
        public void open(OpenContext openContext) {

            totalGmvState =
                    getRuntimeContext().getState(
                            new ValueStateDescriptor<>(
                                    "daily-total-gmv",
                                    BigDecimal.class
                            )
                    );

            nextTimerState =
                    getRuntimeContext().getState(
                            new ValueStateDescriptor<>(
                                    "daily-next-output-timer",
                                    Long.class
                            )
                    );

            dirtyState =
                    getRuntimeContext().getState(
                            new ValueStateDescriptor<>(
                                    "daily-gmv-dirty",
                                    Boolean.class
                            )
                    );
        }

        @Override
        public void processElement(
                GmvDelta delta,
                Context context,
                Collector<DailyGmv> out) throws Exception {

            BigDecimal previousTotal =
                    totalGmvState.value();

            if (previousTotal == null) {
                previousTotal = BigDecimal.ZERO;
            }

            BigDecimal currentTotal =
                    previousTotal.add(
                            delta.deltaAmount
                    );

            /*
             * 更新当天累计GMV。
             */
            totalGmvState.update(
                    currentTotal
            );

            /*
             * 标记当前GMV自上次输出后发生过变化。
             */
            dirtyState.update(true);

            /*
             * 当前日期还没有注册输出定时器时，
             * 注册下一个5秒边界。
             */
            Long currentTimer =
                    nextTimerState.value();

            if (currentTimer == null) {

                long now =
                        context.timerService()
                                .currentProcessingTime();

                long nextTimer =
                        now - now % OUTPUT_INTERVAL_MS
                                + OUTPUT_INTERVAL_MS;

                context.timerService()
                        .registerProcessingTimeTimer(
                                nextTimer
                        );

                nextTimerState.update(
                        nextTimer
                );
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext context,
                Collector<DailyGmv> out) throws Exception {

            Boolean dirty =
                    dirtyState.value();

            BigDecimal currentTotal =
                    totalGmvState.value();

            /*
             * 只有GMV发生过变化时才输出。
             */
            if (Boolean.TRUE.equals(dirty)
                    && currentTotal != null) {

                out.collect(
                        new DailyGmv(
                                context.getCurrentKey(),
                                currentTotal,
                                currentTime()
                        )
                );
            }

            /*
             * 清除当前定时器标记。
             *
             * 下一条数据到来时，
             * 再注册一个新的5秒定时器。
             */
            nextTimerState.clear();
            dirtyState.clear();
        }
    }

    /**
     * DailyGmv转换为Doris JSON。
     */
    public static class DailyGmvJsonProcessFunction
            extends ProcessFunction<DailyGmv, String> {

        private transient ObjectMapper objectMapper;

        @Override
        public void open(OpenContext openContext) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void processElement(
                DailyGmv value,
                Context context,
                Collector<String> out) {

            ObjectNode json =
                    objectMapper.createObjectNode();

            json.put(
                    "biz_date",
                    value.bizDate
            );

            json.put(
                    "gmv",
                    value.gmv
            );

            json.put(
                    "update_time",
                    value.updateTime
            );

            out.collect(
                    json.toString()
            );
        }
    }

    private static DorisSink<String> buildDorisSink() {

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(DORIS_FE_NODES)
                        .setTableIdentifier(DORIS_TABLE)
                        .setUsername(DORIS_USERNAME)
                        .setPassword(DORIS_PASSWORD)
                        .build();

        Properties properties =
                new Properties();

        properties.setProperty(
                "format",
                "json"
        );

        properties.setProperty(
                "read_json_by_line",
                "true"
        );

        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix(
                                "advanced-realtime-gmv-v1"
                        )
                        .setDeletable(false)
                        .setStreamLoadProp(properties)
                        .build();

        DorisSink.Builder<String> builder =
                DorisSink.builder();

        builder
                .setDorisReadOptions(
                        DorisReadOptions.builder().build()
                )
                .setDorisExecutionOptions(
                        executionOptions
                )
                .setSerializer(
                        new SimpleStringSerializer()
                )
                .setDorisOptions(
                        dorisOptions
                );

        return builder.build();
    }

    private static Long parseVersion(
            String updateTime) {

        if (isBlank(updateTime)) {
            return null;
        }

        LocalDateTime localDateTime =
                LocalDateTime.parse(
                        updateTime,
                        DATE_TIME_FORMATTER
                );

        return localDateTime
                .atZone(BUSINESS_ZONE)
                .toInstant()
                .toEpochMilli();
    }

    private static String extractNullableDate(
            String dateTime) {

        if (isBlank(dateTime)) {
            return null;
        }

        if (dateTime.length() < 10) {
            throw new IllegalArgumentException(
                    "时间格式错误：" + dateTime
            );
        }

        return dateTime.substring(0, 10);
    }

    private static BigDecimal zeroIfNull(
            BigDecimal amount) {

        return amount == null
                ? BigDecimal.ZERO
                : amount;
    }

    private static boolean isBlank(String value) {
        return value == null
                || value.trim().isEmpty();
    }

    private static String currentTime() {
        return LocalDateTime
                .now(BUSINESS_ZONE)
                .format(DATE_TIME_FORMATTER);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderDetail implements Serializable {

        @JsonProperty("order_id")
        public String orderId;

        @JsonProperty("user_id")
        public String userId;

        @JsonProperty("sku_id")
        public String skuId;

        @JsonProperty("pay_amount")
        public BigDecimal payAmount;

        @JsonProperty("refund_amount")
        public BigDecimal refundAmount;

        @JsonProperty("order_status")
        public String orderStatus;

        @JsonProperty("create_time")
        public String createTime;

        @JsonProperty("pay_time")
        public String payTime;

        @JsonProperty("dt")
        public String dt;

        /*
         * 推荐上游增加的事件版本时间。
         *
         * 示例：
         * "update_time": "2026-07-31 08:30:15"
         */
        @JsonProperty("update_time")
        public String updateTime;

        public OrderDetail() {
        }
    }

    /**
     * 订单上一次对GMV的贡献状态。
     */
    public static class OrderContributionState
            implements Serializable {

        public String bizDate;
        public BigDecimal contribution;
        public Long lastVersion;

        public OrderContributionState() {
        }

        public OrderContributionState(
                String bizDate,
                BigDecimal contribution,
                Long lastVersion) {

            this.bizDate = bizDate;
            this.contribution = contribution;
            this.lastVersion = lastVersion;
        }
    }

    /**
     * 单个订单产生的GMV变化量。
     */
    public static class GmvDelta implements Serializable {

        public String bizDate;
        public BigDecimal deltaAmount;
        public String orderId;

        public GmvDelta() {
        }

        public GmvDelta(
                String bizDate,
                BigDecimal deltaAmount,
                String orderId) {

            this.bizDate = bizDate;
            this.deltaAmount = deltaAmount;
            this.orderId = orderId;
        }
    }

    /**
     * 每日GMV快照。
     */
    public static class DailyGmv implements Serializable {

        public String bizDate;
        public BigDecimal gmv;
        public String updateTime;

        public DailyGmv() {
        }

        public DailyGmv(
                String bizDate,
                BigDecimal gmv,
                String updateTime) {

            this.bizDate = bizDate;
            this.gmv = gmv;
            this.updateTime = updateTime;
        }
    }
}