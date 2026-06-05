package org.linyu.order;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyu.config.ConfigUtil;
import org.linyu.map.DailyGmvResult;
import org.linyu.map.DailyGmvResultSerializer;
import org.linyu.map.GmvDelta;
import org.linyu.map.OrderGmvContribution;
import org.linyu.map.OrderGmvContributionMapper;
import org.linyu.map.OrderEvent;
import org.linyu.map.OrderEventParser;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 今日实时 GMV 作业。
 *
 * 流程：Kafka 订单明细 -> 按订单状态修正今日交易 GMV -> Flink keyed state 累加 -> 每 5 秒写一条快照到 Doris。
 * Doris 侧 BI 查询最新 stat_time 的一行，就能拿到当前时刻的今日 GMV。
 */
public class day_real_time_gvm {

    private static final long SNAPSHOT_INTERVAL_MS = 5_000L;
    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Doris 2PC sink 依赖 checkpoint 完成事务提交。
        env.enableCheckpointing(
                ConfigUtil.getLong("flink.checkpoint.interval", 5_000L),
                CheckpointingMode.EXACTLY_ONCE
        );
        env.getCheckpointConfig().setCheckpointStorage(
                ConfigUtil.getString(
                        "flink.checkpoint.storage",
                        "file:///tmp/flink-checkpoints/daily-gmv"
                )
        );
        env.getCheckpointConfig().setCheckpointTimeout(2 * 60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigUtil.getString("kafka.bootstrap.servers"))
                .setGroupId(ConfigUtil.getString("kafka.group.id.dagGmv"))
                .setTopics(ConfigUtil.getString("kafka.topic.order"))
                .setStartingOffsets(buildStartingOffsets())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<OrderEvent> orderStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "dwd-order-detail-kafka-source"
                )
                .uid("dwd-order-detail-kafka-source")
                .map(new OrderEventParser())
                .name("parse-order-event")
                .uid("parse-order-event")
                .filter(event -> event != null)
                .name("filter-valid-order-event")
                .uid("filter-valid-order-event");

        orderStream
                // 同一个订单可能从 PAID/FINISHED 变成 CANCELLED，需要按 order_id 做状态修正。
                .keyBy(event -> event.orderId)
                .process(new OrderGmvCorrectionFunction())
                .name("order-gmv-correction")
                .uid("order-gmv-correction")
                // dt 是 pay_time 截出来的交易日期，不使用订单表里的 create_time/dt。
                .keyBy(delta -> delta.dt)
                .process(new DailyGmvAggregateFunction())
                .name("daily-gmv-aggregate")
                .uid("daily-gmv-aggregate")
                .map(new DailyGmvResultSerializer())
                .name("serialize-daily-gmv-result")
                .uid("serialize-daily-gmv-result")
                .sinkTo(buildDorisSink())
                .name("daily-gmv-doris-sink")
                .uid("daily-gmv-doris-sink");

        env.execute("day real time gmv");
    }

    private static OffsetsInitializer buildStartingOffsets() {
        String offsetMode = ConfigUtil.getString("kafka.starting.offsets", "earliest");
        if ("latest".equalsIgnoreCase(offsetMode)) {
            return OffsetsInitializer.latest();
        }
        return OffsetsInitializer.earliest();
    }

    private static DorisSink<String> buildDorisSink() {
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(ConfigUtil.getString("doris.fenodes"))
                .setTableIdentifier(ConfigUtil.getString("doris.table.identifier.dailyGmv"))
                .setUsername(ConfigUtil.getString("doris.username"))
                .setPassword(ConfigUtil.getString("doris.password"))
                .build();

        Properties streamLoadProperties = new Properties();
        streamLoadProperties.setProperty(
                "format",
                ConfigUtil.getString("doris.sink.format", "json")
        );
        streamLoadProperties.setProperty(
                "read_json_by_line",
                ConfigUtil.getString("doris.sink.read.json.by.line", "true")
        );

        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix(
                        ConfigUtil.getString(
                                "doris.sink.label.prefix",
                                "flink_daily_gmv"
                        )
                )
                .setMaxRetries(3)
                // Doris connector 要求 bufferFlushMaxRows >= 10000；低延迟由 interval 控制。
                .setBufferFlushMaxRows(
                        ConfigUtil.getInt("doris.sink.buffer.flush.max.rows", 10_000)
                )
                .setBufferFlushIntervalMs(
                        ConfigUtil.getLong("doris.sink.buffer.flush.interval.ms", 1_000L)
                )
                .setStreamLoadProp(streamLoadProperties)
                .enable2PC()
                .build();

        return DorisSink.<String>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.defaults())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

    public static class OrderGmvCorrectionFunction
            extends KeyedProcessFunction<String, OrderEvent, GmvDelta> {

        private transient ValueState<OrderGmvContribution> contributionState;
        private transient OrderGmvContributionMapper contributionMapper;

        @Override
        public void open(Configuration parameters) {
            contributionMapper = new OrderGmvContributionMapper();

            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.days(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<OrderGmvContribution> descriptor =
                    new ValueStateDescriptor<>(
                            "order-gmv-contribution-state",
                            OrderGmvContribution.class
                    );
            descriptor.enableTimeToLive(ttl);
            contributionState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                OrderEvent event,
                Context ctx,
                Collector<GmvDelta> out
        ) throws Exception {
            OrderGmvContribution oldContribution = contributionState.value();
            OrderGmvContribution newContribution = contributionMapper.map(event);

            if (isSameContribution(oldContribution, newContribution)) {
                return;
            }

            emitReverseDelta(oldContribution, out);
            emitForwardDelta(newContribution, out);

            if (newContribution.isEmpty()) {
                contributionState.clear();
            } else {
                contributionState.update(newContribution);
            }
        }

        private boolean isSameContribution(
                OrderGmvContribution oldContribution,
                OrderGmvContribution newContribution
        ) {
            if (oldContribution == null || oldContribution.isEmpty()) {
                return newContribution == null || newContribution.isEmpty();
            }
            if (newContribution == null || newContribution.isEmpty()) {
                return false;
            }
            return oldContribution.dt.equals(newContribution.dt)
                    && oldContribution.gmv.compareTo(newContribution.gmv) == 0
                    && oldContribution.paidOrderCount == newContribution.paidOrderCount;
        }

        private void emitReverseDelta(
                OrderGmvContribution contribution,
                Collector<GmvDelta> out
        ) {
            if (contribution == null || contribution.isEmpty()) {
                return;
            }
            out.collect(new GmvDelta(
                    contribution.orderId,
                    contribution.dt,
                    contribution.gmv.negate(),
                    -contribution.paidOrderCount
            ));
        }

        private void emitForwardDelta(
                OrderGmvContribution contribution,
                Collector<GmvDelta> out
        ) {
            if (contribution == null || contribution.isEmpty()) {
                return;
            }
            out.collect(new GmvDelta(
                    contribution.orderId,
                    contribution.dt,
                    contribution.gmv,
                    contribution.paidOrderCount
            ));
        }
    }

    /**
     * 每个 dt 一个状态，保存当前这一天已经累计出来的 GMV。
     */
    public static class DailyGmvAggregateFunction
            extends KeyedProcessFunction<String, GmvDelta, DailyGmvResult> {

        private transient ValueState<DailyGmvState> dailyGmvState;

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig
                    .newBuilder(Time.days(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<DailyGmvState> descriptor =
                    new ValueStateDescriptor<>("daily-gmv-state", DailyGmvState.class);
            descriptor.enableTimeToLive(ttl);
            dailyGmvState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                GmvDelta delta,
                Context ctx,
                Collector<DailyGmvResult> out
        ) throws Exception {
            DailyGmvState state = dailyGmvState.value();
            if (state == null) {
                state = new DailyGmvState();
            }

            state.gmv = state.gmv
                    .add(delta.gmvDelta)
                    .setScale(2, RoundingMode.HALF_UP);
            state.paidOrderCount += delta.paidOrderCountDelta;
            if (state.gmv.compareTo(BigDecimal.ZERO) < 0) {
                state.gmv = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
            }
            if (state.paidOrderCount < 0L) {
                state.paidOrderCount = 0L;
            }

            // 第一次收到今日订单时注册定时器，之后每 5 秒输出当前累计快照。
            if (state.nextSnapshotTimer == 0L) {
                state.nextSnapshotTimer =
                        nextSnapshotTime(ctx.timerService().currentProcessingTime());
                ctx.timerService().registerProcessingTimeTimer(state.nextSnapshotTimer);
            }

            dailyGmvState.update(state);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<DailyGmvResult> out
        ) throws Exception {
            DailyGmvState state = dailyGmvState.value();
            if (state == null) {
                return;
            }

            // 到了第二天，昨天的 key 不再继续输出，避免历史日期一直刷 Doris。
            if (!currentBusinessDate().equals(ctx.getCurrentKey())) {
                dailyGmvState.clear();
                return;
            }

            DailyGmvResult result = new DailyGmvResult();
            result.dt = ctx.getCurrentKey();
            result.stat_time = formatDateTime(timestamp);
            result.gmv = state.gmv;
            result.paid_order_count = state.paidOrderCount;
            result.update_time = formatNow();
            out.collect(result);

            state.nextSnapshotTimer = timestamp + SNAPSHOT_INTERVAL_MS;
            dailyGmvState.update(state);
            ctx.timerService().registerProcessingTimeTimer(state.nextSnapshotTimer);
        }

        private long nextSnapshotTime(long currentProcessingTime) {
            return currentProcessingTime
                    - currentProcessingTime % SNAPSHOT_INTERVAL_MS
                    + SNAPSHOT_INTERVAL_MS;
        }
    }

    public static class DailyGmvState implements Serializable {
        public BigDecimal gmv = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        public long paidOrderCount = 0L;
        public long nextSnapshotTimer = 0L;
    }

    private static String currentBusinessDate() {
        return LocalDate.now(BUSINESS_ZONE).toString();
    }

    private static String formatNow() {
        return LocalDateTime.ofInstant(Instant.now(), BUSINESS_ZONE)
                .format(DATE_TIME_FORMATTER);
    }

    private static String formatDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), BUSINESS_ZONE)
                .format(DATE_TIME_FORMATTER);
    }
}
