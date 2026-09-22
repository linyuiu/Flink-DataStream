package org.commerce.job;

import org.commerce.common.MetricGroup;
import org.commerce.config.CommerceConfig;
import org.commerce.source.Connectors;

/**
 * 按业务日和维度计算精确支付人数、退款订单数，独立维护去重状态。
 *
 * <p>并行度由parallelism.distincts指定，容量由用户/订单触达维度的唯一组合数决定。 资源推算及压测要求见JOB_SPLIT_CHECKPOINT_DESIGN.md。
 */
public final class CommerceDistinctMetricsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化去重 Job；大基数去重标记单独做 Checkpoint，可独立扩容和恢复。
        CommerceConfig config = CommerceConfig.load(args);
        var environment = JobEnvironment.create(config, "distincts");

        // 2. 用去重专属消费组读取 DWD，不重复保存公共事实层的 eventId 指纹。
        var tradeFacts =
                SplitJobSupport.factSource(environment, config, "distincts")
                        .uid("commerce-v2-distinct-source");

        // 3. 按“日期 + 维度 + 用户/订单”精确去重，首次出现才产生 +1，再累计分片计数。
        // 本 Job 只计算支付人数和成功退款订单数，不负责 GMV 等金额指标。
        TradePipelines.Streams metricStreams =
                TradePipelines.metrics(tradeFacts, config, MetricGroup.DISTINCT);

        // 4. 写去重专用 Doris 表。ALL 维度已独立去重，不能把不同商品的支付人数相加代替。
        var distinctRows = metricStreams.records;
        distinctRows
                .sinkTo(Connectors.doris(config, "doris.table.distincts", "v2_distincts"))
                .uid("commerce-v2-distinct-doris")
                .setParallelism(Math.toIntExact(config.positive("parallelism.doris")));

        // 5. 保存质量问题和超期补算请求，避免状态到期后的老数据重新进入在线计数。
        SplitJobSupport.writeSideOutputs(metricStreams, config, "distincts");

        // 6. 启动精确去重指标链路。
        environment.execute("Commerce Exact Distinct Metrics");
    }
}
