package org.commerce.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.commerce.config.CommerceConfig;
import org.commerce.source.Connectors;

/**
 * 数据接入入口：MySQL Outbox / 维度表 → CDC 原始消息 → 各表对应的 Kafka ODS Topic。
 *
 * <p>并行度由parallelism.cdc指定。快照分片可以并行，单实例binlog增量读取不能按并行度线性扩容。
 * Checkpoint保存读取进度和分片元数据，资源推算见JOB_SPLIT_CHECKPOINT_DESIGN.md。
 */
public final class CommerceCdcJob {
    public static void main(String[] args) throws Exception {
        // 1. 读取配置，初始化 CDC 作业的并行度、状态后端和 Checkpoint。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "cdc");

        // 2. 接入 MySQL：全新启动先做快照，再订阅 binlog；恢复时由保存的状态接续进度。
        // 此 Job 只同步数据，不做事件时间窗口计算，因此不生成 Watermark。
        var cdcChanges =
                environment
                        .fromSource(
                                Connectors.mysql(config),
                                WatermarkStrategy.noWatermarks(),
                                "commerce-mysql-cdc")
                        .uid("commerce-cdc-source-v1");

        // 3. 按来源表路由到配置中的 Topic，保留 CDC envelope，供下游分别处理事实和维度。
        // Kafka Sink 使用事务写入，数据随成功的 Checkpoint 提交。
        cdcChanges
                .sinkTo(Connectors.kafkaSink(config, new Connectors.CdcRouter(config), "cdc"))
                .uid("commerce-cdc-kafka-v1");

        // 4. 提交拓扑；前面的 Source、Sink 声明本身不会启动消费。
        environment.execute("Commerce CDC Outbox and Dimensions");
    }
}
