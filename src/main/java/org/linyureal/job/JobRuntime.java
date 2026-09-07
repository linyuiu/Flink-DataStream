package org.linyureal.job;

import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.linyureal.config.AppConfig;

public final class JobRuntime {
    private JobRuntime() {}
    public static StreamExecutionEnvironment create(AppConfig c,String job) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Math.toIntExact(c.positive("flink.parallelism")));
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(c.positive("checkpoint.interval.ms"),CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(c.positive("checkpoint.timeout.ms"));
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage(c.get("checkpoint.directory")+"/"+job);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}
