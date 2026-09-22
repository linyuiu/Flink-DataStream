package org.linyureal.realtime.job;

import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.linyureal.realtime.config.PipelineConfig;

public final class Environments {
    private Environments() {}
    public static StreamExecutionEnvironment create(PipelineConfig c, String name) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Math.toIntExact(c.positive("flink.parallelism")));
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(c.positive("checkpoint.interval.ms"), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(c.positive("checkpoint.timeout.ms"));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage(c.get("checkpoint.directory") + "/" + name);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}
