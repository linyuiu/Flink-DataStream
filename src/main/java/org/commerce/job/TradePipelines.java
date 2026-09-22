package org.commerce.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.commerce.common.Json;
import org.commerce.common.MetricGroup;
import org.commerce.config.CommerceConfig;
import org.commerce.function.AdmitAndDeduplicate;
import org.commerce.function.ExactDistinctFunction;
import org.commerce.function.ExpandTradeMetrics;
import org.commerce.function.OutboxParseFunction;
import org.commerce.function.Outputs;
import org.commerce.function.ShardedAccumulator;
import org.commerce.function.TradeFactParseFunction;
import org.commerce.model.MetricDelta;

/**
 * 生产 Job 和本地测试共用的计算拓扑；这里只连接算子，不创建外部 Source/Sink。
 *
 * <p>facts 负责产出唯一业务事实，metrics 负责将事实转成某一组日维度指标。算子 UID 和状态前缀属于恢复契约，不能随可读性整理一并改名。
 */
public final class TradePipelines {
    private TradePipelines() {}

    /** 公共事实链路：Outbox CDC → 解析校验 → 时间准入 / eventId 去重 → 三路输出。 */
    public static Streams facts(DataStream<String> outbox, CommerceConfig config) {
        // 1. 解开 CDC envelope 并校验事实契约；不合法的数据进入质量侧输出。
        var parsedEvents = outbox.process(new OutboxParseFunction()).uid("commerce-v2-fact-parser");

        // 2. 按事实中的 eventId 分区，不依赖 Kafka key；同 ID 同内容是重复，不同内容是冲突。
        // 去重算子还检查业务时间准入，超期数据进入 repair，避免 TTL 清理后再次入账。
        var acceptedEvents =
                parsedEvents
                        .keyBy(raw -> Json.text(Json.object(raw), "eventId"))
                        .process(new AdmitAndDeduplicate(config.positive("online.event.days")))
                        .uid("commerce-v2-fact-dedup");

        // 3. 汇总各阶段的质量问题，同时保留正常事实和待补算数据；出口由 Job 负责连接。
        DataStream<String> qualityRecords =
                parsedEvents
                        .getSideOutput(Outputs.QUALITY)
                        .union(acceptedEvents.getSideOutput(Outputs.QUALITY));
        return new Streams(
                acceptedEvents, qualityRecords, acceptedEvents.getSideOutput(Outputs.REPAIR));
    }

    /** 指标链路：DWD 事实 → 维度增量 → 金额 / 精确去重分支 → 日维度分片累计。 */
    public static Streams metrics(
            DataStream<String> facts, CommerceConfig config, MetricGroup group) {
        // 1. 复核 DWD 契约，不再维护一套 eventId 去重状态；金额和去重组使用各自稳定的 UID。
        String operatorPrefix = "commerce-v2-" + group.statePrefix();
        long eventDays = config.positive("online.event.days");
        var validatedFacts =
                facts.process(new TradeFactParseFunction(eventDays))
                        .uid(operatorPrefix + "-parser");

        // 2. 一条事实展开为多个业务日期/维度的增量；不同基数的维度使用不同分片数。
        // 返回值既含金额增量，也含待去重的用户/订单标记，下一步只保留当前指标组所需数据。
        var dimensionDeltas =
                validatedFacts
                        .flatMap(new ExpandTradeMetrics(config))
                        .returns(MetricDelta.class)
                        .uid(operatorPrefix + "-expand");

        // 3. 按指标组选择处理方式；分支在构建拓扑时确定，并非每条消息都重新选择一个 Job。
        SingleOutputStreamOperator<MetricDelta> increments;
        DataStream<String> repairs = validatedFacts.getSideOutput(Outputs.REPAIR);
        long aggregateDays;
        if (group == MetricGroup.AMOUNT) {
            // 金额和可加计数无需用户级去重，直接累计；保留较长周期以支持退款回扣原支付日。
            increments =
                    dimensionDeltas
                            .filter(delta -> delta.distinctKind == null)
                            .uid(operatorPrefix + "-filter");
            aggregateDays = config.positive("online.metric.days");
        } else {
            // “日期 + 维度 + 去重类型 + 用户/订单”首次出现才输出 +1，重复标记不输出增量。
            var uniqueIncrements =
                    dimensionDeltas
                            .filter(delta -> delta.distinctKind != null)
                            .uid(operatorPrefix + "-filter")
                            .keyBy(MetricDelta::distinctKey)
                            .process(new ExactDistinctFunction(eventDays))
                            .uid(operatorPrefix + "-exact-distinct");
            increments = uniqueIncrements;
            repairs = repairs.union(uniqueIncrements.getSideOutput(Outputs.REPAIR));

            // 日 UV 不回改原支付日，计数累计只需覆盖事件准入范围，无需保留 93 天。
            aggregateDays =
                    config.positive(
                            "online.distinct.metric.days",
                            config.positive("online.event.days") + 2);
        }

        // 4. 按“业务日期 + 维度类型 + 维度 ID + 分片”累计当前组指标，定时输出绝对小计。
        // flush.interval.ms 控制算子输出周期，Doris 最终可见时间还受 Sink / Checkpoint 提交影响。
        var partialTotals =
                increments
                        .keyBy(MetricDelta::partitionKey)
                        .process(
                                new ShardedAccumulator(
                                        aggregateDays, config.positive("flush.interval.ms"), group))
                        .uid(operatorPrefix + "-aggregate");

        // 5. 合并去重阶段和累计阶段的超期数据；业务 Job 分别连接正常、质量、补算三个出口。
        repairs = repairs.union(partialTotals.getSideOutput(Outputs.REPAIR));
        return new Streams(partialTotals, validatedFacts.getSideOutput(Outputs.QUALITY), repairs);
    }

    /** 三路输出显式分开，避免调用方只连接正常结果而遗漏异常数据。 */
    public static final class Streams {
        /** 正常记录：事实链路输出 DWD JSON，指标链路输出 Doris 分片累计 JSON。 */
        public final SingleOutputStreamOperator<String> records;

        /** 质量问题：格式错误、契约不符或不可变内容冲突等。 */
        public final DataStream<String> quality;

        /** 待补算记录：超过在线处理期限，需要走受控历史修复流程。 */
        public final DataStream<String> repair;

        private Streams(
                SingleOutputStreamOperator<String> records,
                DataStream<String> quality,
                DataStream<String> repair) {
            this.records = records;
            this.quality = quality;
            this.repair = repair;
        }
    }
}
