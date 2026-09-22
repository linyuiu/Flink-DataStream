package org.commerce.job;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.commerce.config.CommerceConfig;

/** 统一初始化运行环境；业务 Job 只指定自己的配置前缀，不重复设置恢复策略。 */
public final class JobEnvironment {
    private JobEnvironment() {}

    public static StreamExecutionEnvironment create(CommerceConfig config, String jobName) {
        config.validateJob(jobName);
        // 1. 设置该 Job 的并行度和最大并行度；最大并行度影响 keyed state 的 key-group 划分。
        // 默认 slot sharing 下，一个 Job 的 slot 需求通常取最大算子并行度，不是把 Source/Map/Sink相加。
        // 分作业容量模型见资源文档；slot调度数和CPU/内存需求需要分别核算。
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(Math.toIntExact(config.positive("parallelism." + jobName)));
        environment.setMaxParallelism(Math.toIntExact(config.positive("flink.max.parallelism")));

        // 2. 使用 RocksDB 增量快照，并将定时器队列放入 RocksDB，减轻大量日维度 key 的堆压力。
        // 增量快照复用未变化的文件，不代表逻辑状态不会增长，也不代表每次 CK 都很小。
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
        backend.setPriorityQueueStateType(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        environment.setStateBackend(backend);

        // 3. 优先使用当前 Job 的 Checkpoint 周期；未单独配置时回退到全局周期。
        // EXACTLY_ONCE 负责 Flink 状态一致性，外部可见性还依赖 Connector 的事务提交。
        long checkpointIntervalMillis =
                config.positive(
                        "checkpoint." + jobName + ".interval.ms",
                        config.positive("checkpoint.interval.ms"));
        environment.enableCheckpointing(checkpointIntervalMillis, CheckpointingMode.EXACTLY_ONCE);

        // 4. 限制快照并发并设置超时，避免多个 Checkpoint 同时争抢磁盘/上传资源。
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(config.positive("checkpoint.timeout.ms"));
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 5. 各 Job 使用独立目录，手动取消后保留外部快照；重新提交时仍需显式选择恢复路径。
        // 保留快照不代表任意新拓扑都能恢复，算子 UID、状态结构等仍须兼容。
        checkpointConfig.setCheckpointStorage(config.get("checkpoint.directory") + "/" + jobName);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return environment;
    }
}
