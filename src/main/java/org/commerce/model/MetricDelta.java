package org.commerce.model;

import org.commerce.common.Metric;

/** 一条日维度增量。去重请求只有首次出现时才转成数值增量；字段结构需兼容历史 Flink 状态。 */
public class MetricDelta {
    public String date;
    public String dimensionType;
    public String dimensionId;
    public String partitionEntity;
    public String distinctKind;
    public String distinctEntity;
    public int shard;
    public long[] values;

    public MetricDelta() {}

    public void set(Metric metric, long value) {
        values[metric.index()] = value;
    }

    public long value(Metric metric) {
        return values[metric.index()];
    }

    public String partitionKey() {
        return date + "|" + dimensionType + "|" + dimensionId + "|" + shard;
    }

    public String distinctKey() {
        return date
                + "|"
                + dimensionType
                + "|"
                + dimensionId
                + "|"
                + distinctKind
                + "|"
                + distinctEntity;
    }
}
