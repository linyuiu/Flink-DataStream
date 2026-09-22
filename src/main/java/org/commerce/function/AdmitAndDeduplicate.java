package org.commerce.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.commerce.common.Json;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 事实准入与幂等去重算子。
 *
 * <p>上游必须先按 {@code eventId} 执行 {@code
 * keyBy}，这样相同事件ID始终访问同一份状态。本算子依次完成：校验事件时间、隔离超期事件、比较内容指纹、过滤重复事件，并将首次出现的合法事实输出给下游。
 *
 * <p>时间准入必须早于状态去重。即使eventId指纹因TTL到期被清理，已经超过在线处理期限的老事件仍会进入repair，不会从空状态重新入账。
 *
 * <p>主要生命周期：
 *
 * <ol>
 *   <li>构造函数：在构建作业拓扑时保存普通配置，例如{@code eventDays}；
 *   <li>{@link #open(OpenContext)}：TaskManager创建每个并行子任务实例后调用一次，用于绑定状态和创建监控指标；
 *   <li>{@link #processElement(String, Context, Collector)}：open完成后，每到一条消息调用一次，执行真正的准入和去重；
 * </ol>
 *
 * <p>因此不能在构造函数中调用{@code getRuntimeContext().getState(...)}：构造时Flink运行时上下文和状态后端还没有准备完成。
 */
public class AdmitAndDeduplicate extends KeyedProcessFunction<String, String, String> {
    private static final long MAX_FUTURE_SKEW_MILLIS = 5 * 60_000L;

    /** 允许在线处理的业务日期范围，单位为天。 */
    private final long eventDays;

    /** 当前eventId已经接纳的内容指纹；由于上游按eventId分区，每个key只保存一个指纹。 */
    private transient ValueState<String> fingerprint;

    /** 内容完全相同的重复事件数量，用于运行监控。 */
    private transient Counter duplicateEvents;

    /** 超过在线处理期限、需要历史补算的事件数量。 */
    private transient Counter repairEvents;

    private transient Counter acceptedEvents;
    private transient Counter conflictingEvents;
    private transient Counter futureEvents;

    /**
     * 创建事实准入与去重算子。
     *
     * @param eventDays 可以在线接纳的业务日期天数；状态TTL会在此基础上额外保留2天
     */
    public AdmitAndDeduplicate(long eventDays) {
        this.eventDays = eventDays;
    }

    /**
     * 在处理数据之前，为当前并行子任务初始化运行时资源。
     *
     * <p>正常情况下，Flink会先调用一次{@code open}，然后才会多次调用{@code
     * processElement}。这里的“初始化状态”不是把业务状态设置为空，而是通过状态描述符从状态后端取得当前key对应状态的访问句柄：
     *
     * <ul>
     *   <li>全新启动时，尚未出现过的eventId读取结果为null；
     *   <li>从兼容Checkpoint/Savepoint恢复时，句柄会访问已经恢复的指纹数据；
     *   <li>每个并行子任务只调用一次open，不会每来一条消息就重新创建状态。
     * </ul>
     *
     * <p>指纹使用处理时间TTL，保留{@code eventDays + 2}天。状态只在首次写入时更新；过期状态不可见，并由RocksDB
     * compaction逐步清理。状态名称是Checkpoint兼容契约，不能随意修改。
     */
    @Override
    public void open(OpenContext context) {
        // 1. 声明状态的名称和序列化类型。Flink使用该描述符绑定新状态或恢复同名兼容状态。
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("event-fingerprint-v1", String.class);

        // 2. 为指纹配置处理时间TTL。TTL到期后状态对业务代码不可见，物理数据由RocksDB逐步回收。
        descriptor.enableTimeToLive(
                StateTtlConfig.newBuilder(java.time.Duration.ofDays(eventDays + 2))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .cleanupInRocksdbCompactFilter(1000)
                        .build());

        // 3. 获取状态访问句柄。它会随当前key切换到对应指纹；恢复任务时不会覆盖Checkpoint中的值。
        fingerprint = getRuntimeContext().getState(descriptor);

        // 4. 注册当前并行子任务的监控计数器，后续每条消息处理时只做递增。
        duplicateEvents = getRuntimeContext().getMetricGroup().counter("duplicate_events");
        repairEvents = getRuntimeContext().getMetricGroup().counter("repair_events");
        acceptedEvents = getRuntimeContext().getMetricGroup().counter("accepted_events");
        conflictingEvents = getRuntimeContext().getMetricGroup().counter("conflicting_events");
        futureEvents = getRuntimeContext().getMetricGroup().counter("future_events");
    }

    /**
     * 处理一条已经按eventId分区的交易事实。
     *
     * <p>处理结果分为四类：
     *
     * <ol>
     *   <li>事件时间超过当前时间5分钟：写入quality侧输出；
     *   <li>业务日期超过在线准入范围：写入repair侧输出；
     *   <li>eventId已存在：相同内容作为重复消息过滤，不同内容作为不可变事实冲突写入quality；
     *   <li>首次出现的合法事件：保存SHA-256内容指纹，并输出规范化事件JSON。
     * </ol>
     *
     * @param raw 上游解析并完成基础契约校验的交易事实JSON
     * @param context Flink keyed process上下文，用于读取处理时间和发送侧输出
     * @param output 正常事实主输出；重复、质量问题和超期事件不会写入此输出
     */
    @Override
    public void processElement(String raw, Context context, Collector<String> output)
            throws Exception {
        // 1. 再次按业务契约解析事件，并取得Flink处理时间作为在线准入判断基准。
        TradeEvent event = EventContract.parse(raw);
        long now = context.timerService().currentProcessingTime();

        // 2. 业务发生时间明显领先处理时间，通常表示来源时钟或字段单位错误，进入质量流。
        if (event.occurredAt > now + MAX_FUTURE_SKEW_MILLIS) {
            futureEvents.inc();
            context.output(
                    Outputs.QUALITY,
                    Outputs.record(
                            "future-event", raw, "Business event is over 5 minutes in the future"));
            return;
        }

        // 3. 超出在线日期范围的数据不能从当前状态继续累计，送入受控补算流程。
        String date = Horizons.date(event.occurredAt);
        if (!Horizons.open(date, now, eventDays)) {
            repairEvents.inc();
            context.output(
                    Outputs.REPAIR,
                    Outputs.record(
                            "event-horizon",
                            raw,
                            "Outside online admission horizon; controlled backfill required"));
            return;
        }

        // 4. 对完整事实内容计算指纹。eventId相同且指纹相同是重放；指纹不同是不可变事实冲突。
        String currentFingerprint =
                java.util.Base64.getEncoder()
                        .encodeToString(
                                MessageDigest.getInstance("SHA-256")
                                        .digest(raw.getBytes(StandardCharsets.UTF_8)));
        String previousFingerprint = fingerprint.value();
        if (previousFingerprint != null) {
            if (previousFingerprint.equals(currentFingerprint)) {
                // 重复消息只记录指标，不再次输出，因此不会重复累计GMV。
                duplicateEvents.inc();
            } else {
                conflictingEvents.inc();
                // 同一eventId不允许修改内容，否则无法确定哪个版本才是权威事实。
                context.output(
                        Outputs.QUALITY,
                        Outputs.record("event-conflict", raw, "Same immutable event ID changed"));
            }
            return;
        }

        // 5. 首次合法事件先保存指纹，再输出规范化JSON；状态和下游进度由Checkpoint一起恢复。
        fingerprint.update(currentFingerprint);
        acceptedEvents.inc();
        output.collect(Json.write(event));
    }
}
