package org.commerce.job;

import org.commerce.common.Json;
import org.commerce.config.CommerceConfig;
import org.commerce.function.Horizons;
import org.commerce.function.Outputs;
import org.commerce.function.TradeFactParseFunction;
import org.commerce.source.Connectors;
import org.commerce.validation.EventContract;

/**
 * 审计写入隔离：Doris 明细导入变慢时不阻塞公共事实发布或金额 Job 的 checkpoint。
 *
 * <p>并行度由parallelism.audit指定。Checkpoint主要保存消费位点和导入事务信息， 审计明细长期存储容量与Checkpoint大小分别核算。
 */
public final class CommerceAuditJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化独立审计 Job，让明细写入与指标计算拥有各自的消费进度和 Checkpoint。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "audit");

        // 2. 读取 DWD 事实并校验契约；上游已经完成 eventId 去重，这里不再复制去重状态。
        var validatedFacts =
                SplitJobSupport.factSource(environment, config, "audit")
                        .uid("commerce-v2-audit-source")
                        .process(new TradeFactParseFunction())
                        .uid("commerce-v2-audit-parser");

        // 3. 提取审计检索字段，完整保留原始事实 JSON，便于逐笔核对指标来源。
        var auditRows =
                validatedFacts
                        .map(CommerceAuditJob::auditRow)
                        .returns(String.class)
                        .uid("commerce-v2-audit-json");

        // 4. 将明细写入 Doris 审计表；此处不做 GMV 汇总，也不回写金额/去重结果表。
        auditRows
                .sinkTo(Connectors.doris(config, "doris.table.events", "v2_audit"))
                .uid("commerce-v2-audit-doris")
                .setParallelism(Math.toIntExact(config.positive("parallelism.audit")));

        // 5. 不合法的 DWD 数据进入质量 Topic，保留排错依据。
        validatedFacts
                .getSideOutput(Outputs.QUALITY)
                .map(record -> Outputs.forJob(record, "audit"))
                .uid("commerce-v2-audit-quality-context")
                .sinkTo(
                        Connectors.kafkaSink(
                                config, config.topic("split_quality"), "v2-audit-quality"))
                .uid("commerce-v2-audit-quality");

        // 6. 启动审计链路。
        environment.execute("Commerce Trade Event Audit");
    }

    /** 按事件发生日组织审计记录；退款事件仍保留自身发生日，不改成原支付日。 */
    public static String auditRow(String raw) {
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
