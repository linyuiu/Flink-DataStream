package org.commerce.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.commerce.config.CommerceConfig;
import org.commerce.source.Connectors;

import java.util.List;

/**
 * 公共事实层：只维护一份 eventId 去重状态，输出事务性 Kafka DWD 流。
 *
 * <p>并行度由parallelism.facts指定。指纹数量由事件速率和在线准入范围决定， 资源推算及压测要求见JOB_SPLIT_CHECKPOINT_DESIGN.md。
 */
public final class CommerceFactJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化公共事实 Job；它有独立的消费组、状态目录和 Checkpoint 周期。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "facts");

        // 2. 读取 Outbox 的 CDC 消息。Topic 从配置读取，不直接消费订单表或支付流水表。
        var outboxChanges =
                environment
                        .fromSource(
                                Connectors.kafka(
                                        config,
                                        List.of(config.topic("trade_outbox")),
                                        "group.facts"),
                                WatermarkStrategy.noWatermarks(),
                                "commerce-v2-outbox")
                        .uid("commerce-v2-outbox-source");

        // 3. 解开 CDC envelope，校验事实、检查时间准入，并按 eventId 去重。
        // 正常事实、质量问题、超期补算请求通过三条流分别返回。
        TradePipelines.Streams factStreams = TradePipelines.facts(outboxChanges, config);

        // 4. 发布统一 DWD 事实；金额、去重、审计 Job 各自消费这份已清洗的事实流。
        var canonicalFacts = factStreams.records;
        canonicalFacts
                .sinkTo(
                        Connectors.kafkaSink(
                                config,
                                new Connectors.TradeFactSerializer(config.topic("trade_fact")),
                                "v2-facts"))
                .uid("commerce-v2-fact-kafka-sink");

        // 5. 将异常和超期数据写入对应 Kafka Topic，供告警、排查和受控补算。
        SplitJobSupport.writeSideOutputs(factStreams, config, "facts");

        // 6. 启动公共事实链路。
        environment.execute("Commerce Canonical Trade Facts");
    }
}
