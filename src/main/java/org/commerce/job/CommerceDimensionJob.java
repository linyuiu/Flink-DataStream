package org.commerce.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.commerce.common.Contract;
import org.commerce.config.CommerceConfig;
import org.commerce.function.DimensionFunctions;
import org.commerce.function.Outputs;
import org.commerce.source.Connectors;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 维度入口：Kafka 维度 CDC → 解析与版本判定 → Doris 最新维度目录。
 *
 * <p>并行度由parallelism.dimensions指定。状态随主数据总行数及JSON大小增长，
 * 停用维度保留历史可查询身份；资源推算见JOB_SPLIT_CHECKPOINT_DESIGN.md。
 */
public final class CommerceDimensionJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化维度 Job；这里维护显示信息，不重新分配历史交易的维度归属。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "dimensions");

        // 2. 从受支持的 CDC 表中挑出维度表，解析为配置中的 Topic 列表。
        List<String> dimensionTopics =
                Contract.CDC_TABLES.stream()
                        .filter(table -> table.startsWith("dim_"))
                        .map(config::topic)
                        .collect(Collectors.toList());
        var dimensionChanges =
                environment
                        .fromSource(
                                Connectors.kafka(config, dimensionTopics, "group.dimensions"),
                                WatermarkStrategy.noWatermarks(),
                                "commerce-dimensions")
                        .uid("commerce-dimension-source-v1");

        // 3. 将各表的 CDC 消息转成统一维度结构；不支持的操作和格式错误进入 quality。
        var parsedDimensions =
                dimensionChanges
                        .process(new DimensionFunctions.Parse())
                        .uid("commerce-dimension-parse-v1");

        // 4. 按“维度类型 + 维度 ID”保存最新版本：旧版本/重复版本不更新，同版本冲突报质量问题。
        var latestDimensions =
                parsedDimensions
                        .keyBy(DimensionFunctions::key)
                        .process(new DimensionFunctions.Latest())
                        .uid("commerce-dimension-latest-v1");

        // 5. 写入 Doris 最新维度目录，供报表关联名称等展示属性。
        latestDimensions
                .sinkTo(Connectors.doris(config, "doris.table.dimensions", "dimensions"))
                .uid("commerce-dimensions-doris-v1");

        // 6. 汇总解析和版本判断产生的质量问题，保留到 Kafka 供监控、排查。
        parsedDimensions
                .getSideOutput(Outputs.QUALITY)
                .union(latestDimensions.getSideOutput(Outputs.QUALITY))
                .map(record -> Outputs.forJob(record, "dimensions"))
                .uid("commerce-dimension-quality-context-v1")
                .sinkTo(Connectors.kafkaSink(config, config.topic("quality"), "dimension-quality"))
                .uid("commerce-dimension-quality-v1");

        // 7. 启动维度链路。
        environment.execute("Commerce Latest Dimension Catalog");
    }
}
