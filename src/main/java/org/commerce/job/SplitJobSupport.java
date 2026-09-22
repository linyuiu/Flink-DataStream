package org.commerce.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.commerce.config.CommerceConfig;
import org.commerce.function.Outputs;
import org.commerce.source.Connectors;

import java.util.List;

/** 拆分 Job 的输入输出边界，不包含业务计算。 */
final class SplitJobSupport {
    private SplitJobSupport() {}

    /** 相同 DWD Topic、各 Job 独立消费组；消费位点随各自的 Checkpoint 恢复。 */
    static DataStreamSource<String> factSource(
            StreamExecutionEnvironment environment, CommerceConfig config, String job) {
        // Connector 使用 read_committed，只消费上游 Kafka 事务已提交的事实。
        // 业务日期由事实字段解析；这里没有窗口算子，不依赖 Watermark 驱动输出。
        return environment.fromSource(
                Connectors.kafka(config, List.of(config.topic("trade_fact")), "group." + job),
                WatermarkStrategy.noWatermarks(),
                "commerce-v2-" + job + "-source");
    }

    /** 为业务拓扑接上异常出口；写入 Topic 只是保留问题，不会自动修复或自动补算。 */
    static void writeSideOutputs(
            TradePipelines.Streams streams, CommerceConfig config, String job) {
        // quality：格式、契约或内容冲突等问题，需定位来源并修正。
        streams.quality
                .map(record -> Outputs.forJob(record, job))
                .uid("commerce-v2-" + job + "-quality-context")
                .sinkTo(
                        Connectors.kafkaSink(
                                config, config.topic("split_quality"), "v2-" + job + "-quality"))
                .uid("commerce-v2-" + job + "-quality-sink");

        // repair：超出在线准入/状态保留范围的数据，交给受控历史补算流程。
        // 事务前缀和算子 UID 均带 Job 标识，不与其他拆分作业混用。
        streams.repair
                .map(record -> Outputs.forJob(record, job))
                .uid("commerce-v2-" + job + "-repair-context")
                .sinkTo(
                        Connectors.kafkaSink(
                                config, config.topic("split_repair"), "v2-" + job + "-repair"))
                .uid("commerce-v2-" + job + "-repair-sink");
    }
}
