package org.commerce.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;
import org.commerce.function.AdmitAndDeduplicate;
import org.commerce.function.ExactDistinctFunction;
import org.commerce.function.ExpandTradeMetrics;
import org.commerce.function.Horizons;
import org.commerce.function.OutboxParseFunction;
import org.commerce.function.Outputs;
import org.commerce.function.ShardedAccumulator;
import org.commerce.model.MetricDelta;
import org.commerce.source.Connectors;
import org.commerce.validation.EventContract;

import java.util.List;

/**
 * 集成指标入口：Outbox → 校验去重 → 展开维度 → 分片累计 / 审计 → Doris。
 *
 * <p>仅在pipeline.mode=integrated时启动，适用于需要同一Checkpoint协调指标与审计的部署。
 * 分拆作业使用pipeline.mode=split；同一套输出表不能由两套作业共同维护。
 */
public final class CommerceMetricsJob {
    public static void main(String[] args) throws Exception {
        // 1. 校验部署模式，避免误提交第二套指标写入链路。
        CommerceConfig config = CommerceConfig.load(args);
        if (!"integrated".equals(config.get("pipeline.mode", "split"))) {
            throw new IllegalArgumentException(
                    "CommerceMetricsJob requires pipeline.mode=integrated");
        }
        var environment = JobEnvironment.create(config, "metrics");

        // 2. 读取并解析 MySQL Outbox 的 CDC 消息；Topic 和消费组从配置中读取。
        var outboxChanges =
                environment
                        .fromSource(
                                Connectors.kafka(
                                        config,
                                        List.of(config.topic("trade_outbox")),
                                        "group.metrics"),
                                WatermarkStrategy.noWatermarks(),
                                "commerce-outbox")
                        .uid("commerce-outbox-source-v1");
        var parsedEvents =
                outboxChanges.process(new OutboxParseFunction()).uid("commerce-event-parser-v1");

        // 3. 按业务 eventId 去重，不依赖 Kafka key；超期事实进入 repair。
        var acceptedEvents =
                parsedEvents
                        .keyBy(raw -> Json.text(Json.object(raw), "eventId"))
                        .process(new AdmitAndDeduplicate(config.positive("online.event.days")))
                        .uid("commerce-event-dedup-v1");

        // 4. 展开日维度增量；金额可直接累计，支付人数和退款订单数须先精确去重。
        var dimensionDeltas = expandDimensions(acceptedEvents, config);
        var distinctDeltas =
                dimensionDeltas
                        .filter(delta -> delta.distinctKind != null)
                        .uid("commerce-distinct-filter-v1")
                        .keyBy(MetricDelta::distinctKey)
                        .process(new ExactDistinctFunction(config.positive("online.event.days")))
                        .uid("commerce-distinct-v1");

        // 5. 将首次去重产生的计数增量与金额增量合并，按日维度分片累计。
        var partialTotals =
                dimensionDeltas
                        .filter(delta -> delta.distinctKind == null)
                        .uid("commerce-monetary-filter-v1")
                        .union(distinctDeltas)
                        .keyBy(MetricDelta::partitionKey)
                        .process(
                                new ShardedAccumulator(
                                        config.positive("online.metric.days"),
                                        config.positive("flush.interval.ms")))
                        .uid("commerce-partial-aggregate-v1");

        // 6. 分别写入指标小计和事实审计明细，两个Doris出口属于同一个Job。
        partialTotals
                .sinkTo(Connectors.doris(config, "doris.table.partials", "partials"))
                .uid("commerce-partials-doris-v1")
                .setParallelism(Math.toIntExact(config.positive("parallelism.doris")));
        writeAuditEvents(acceptedEvents, config);

        // 7. 分开保存无效数据和待补算数据；侧输出不等于自动修复，运行时需要监控。
        parsedEvents
                .getSideOutput(Outputs.QUALITY)
                .union(acceptedEvents.getSideOutput(Outputs.QUALITY))
                .map(record -> Outputs.forJob(record, "metrics"))
                .uid("commerce-quality-context-v1")
                .sinkTo(Connectors.kafkaSink(config, config.topic("quality"), "quality"))
                .uid("commerce-quality-v1");
        acceptedEvents
                .getSideOutput(Outputs.REPAIR)
                .union(
                        distinctDeltas.getSideOutput(Outputs.REPAIR),
                        partialTotals.getSideOutput(Outputs.REPAIR))
                .map(record -> Outputs.forJob(record, "metrics"))
                .uid("commerce-repair-context-v1")
                .sinkTo(Connectors.kafkaSink(config, config.topic("repair"), "repair"))
                .uid("commerce-repair-v1");

        // 8. 启动集成指标链路。
        environment.execute("Commerce Sharded Realtime Metrics");
    }

    /** 将一条事实展开为多个日期/维度增量，按配置分片，避免所有指标集中到一个全站 key。 */
    private static SingleOutputStreamOperator<MetricDelta> expandDimensions(
            SingleOutputStreamOperator<String> events, CommerceConfig config) {
        return events.flatMap(new ExpandTradeMetrics(config))
                .returns(MetricDelta.class)
                .uid("commerce-expand-v1");
    }

    /** 单独保留已接纳的唯一事实，便于从汇总结果反查原始事件。 */
    private static void writeAuditEvents(
            SingleOutputStreamOperator<String> events, CommerceConfig config) {
        events.map(CommerceMetricsJob::toAuditRow)
                .returns(String.class)
                .uid("commerce-audit-json-v1")
                .sinkTo(Connectors.doris(config, "doris.table.events", "events"))
                .uid("commerce-events-doris-v1")
                .setParallelism(Math.toIntExact(config.positive("parallelism.doris")));
    }

    /** 提取检索字段，完整事实放在 payload_json；日期使用事件发生日。 */
    private static String toAuditRow(String raw) {
        var event = EventContract.parse(raw);
        return Json.MAPPER
                .createObjectNode()
                .put("event_date", Horizons.date(event.occurredAt))
                .put("event_id", event.eventId)
                .put("event_type", event.eventType)
                .put("order_id", event.orderId)
                .put("payload_json", raw)
                .toString();
    }
}
