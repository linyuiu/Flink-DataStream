package org.linyu.config;

public final class RealtimeGmvConfig {

    private final KafkaSourceConfig kafka;
    private final DorisSinkConfig doris;
    private final GmvPolicy gmvPolicy;

    private final int parallelism;
    private final long checkpointIntervalMs;
    private final long checkpointTimeoutMs;
    private final String checkpointDirectory;

    private final long outputIntervalMs;
    private final int orderStateTtlDays;
    private final int dailyStateTtlDays;

    public RealtimeGmvConfig(
            KafkaSourceConfig kafka,
            DorisSinkConfig doris,
            GmvPolicy gmvPolicy,
            int parallelism,
            long checkpointIntervalMs,
            long checkpointTimeoutMs,
            String checkpointDirectory,
            long outputIntervalMs,
            int orderStateTtlDays,
            int dailyStateTtlDays
    ) {
        this.kafka = kafka;
        this.doris = doris;
        this.gmvPolicy = gmvPolicy;
        this.parallelism = parallelism;
        this.checkpointIntervalMs =
                checkpointIntervalMs;
        this.checkpointTimeoutMs =
                checkpointTimeoutMs;
        this.checkpointDirectory =
                checkpointDirectory;
        this.outputIntervalMs =
                outputIntervalMs;
        this.orderStateTtlDays =
                orderStateTtlDays;
        this.dailyStateTtlDays =
                dailyStateTtlDays;
    }

    // 此处生成全部字段的 getter，不需要 setter。


    public KafkaSourceConfig getKafka() {
        return kafka;
    }

    public DorisSinkConfig getDoris() {
        return doris;
    }

    public GmvPolicy getGmvPolicy() {
        return gmvPolicy;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public long getCheckpointTimeoutMs() {
        return checkpointTimeoutMs;
    }

    public String getCheckpointDirectory() {
        return checkpointDirectory;
    }

    public long getOutputIntervalMs() {
        return outputIntervalMs;
    }

    public int getOrderStateTtlDays() {
        return orderStateTtlDays;
    }

    public int getDailyStateTtlDays() {
        return dailyStateTtlDays;
    }
}