package org.linyureal.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.linyureal.config.AppConfig;
import java.util.List;

public final class KafkaSources {
    private KafkaSources() {}
    public static KafkaSource<String> create(AppConfig c,List<String> topics,String group) {
        // Fresh jobs rebuild from retained history. Normal restarts MUST restore Flink state.
        return KafkaSource.<String>builder().setBootstrapServers(c.get("kafka.bootstrap.servers"))
                .setTopics(topics).setGroupId(group).setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level","read_committed")
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
    }
}
