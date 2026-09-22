package org.commerce.job;

import org.commerce.common.MetricGroup;
import org.commerce.config.CommerceConfig;
import org.commerce.source.Connectors;

/**
 * 计算商品支付GMV、退款额及订单计数，按业务日、维度和分片写出累计结果。
 *
 * <p>并行度由parallelism.amounts指定。状态容量取决于活跃日期、维度基数和实际分片数； 资源推算及压测要求见JOB_SPLIT_CHECKPOINT_DESIGN.md。
 */
public final class CommerceAmountMetricsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化金额 Job，独立配置并行度和 Checkpoint，不与精确去重共享作业状态。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "amounts");

        // 2. 使用金额专属消费组读取 DWD 事实，避免与其他 Job 共用消费进度。
        var tradeFacts =
                SplitJobSupport.factSource(environment, config, "amounts")
                        .uid("commerce-v2-amount-source");

        // 3. 展开业务日期和维度，计算 9 项可直接累加的金额/计数，并维护日维度分片小计。
        // 退款额记退款日，净 GMV 的扣减归原支付日；具体口径在 EventExpansion 中实现。
        TradePipelines.Streams metricStreams =
                TradePipelines.metrics(tradeFacts, config, MetricGroup.AMOUNT);

        // 4. 写金额专用 Doris 表，输出的是分片累计值而非增量；全维度总计由查询层 SUM。
        // 与去重表分开存储，避免两个 Job 对同一行的全字段更新互相覆盖。
        var amountRows = metricStreams.records;
        amountRows
                .sinkTo(Connectors.doris(config, "doris.table.amounts", "v2_amounts"))
                .uid("commerce-v2-amount-doris")
                .setParallelism(Math.toIntExact(config.positive("parallelism.doris")));

        // 5. 保存格式异常及已超过在线处理期限的数据，不在在线状态中强行重建历史小计。
        SplitJobSupport.writeSideOutputs(metricStreams, config, "amounts");

        // 6. 启动金额指标链路。
        environment.execute("Commerce Amount and Order Metrics");
    }
}
