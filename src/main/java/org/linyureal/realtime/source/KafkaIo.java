package org.linyureal.realtime.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.*;
import org.linyureal.realtime.config.PipelineConfig;
import java.util.List;

public final class KafkaIo {
    private KafkaIo() {}
    public static KafkaSource<String> source(PipelineConfig c, List<String> topics, String group) {
        return KafkaSource.<String>builder().setBootstrapServers(c.get("kafka.bootstrap.servers"))
                .setTopics(topics).setGroupId(c.get(group)).setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
    }
    public static KafkaSink<String> sink(PipelineConfig c, String topic, String suffix) {
        return sink(c, KafkaRecordSerializationSchema.<String>builder().setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema()).build(), suffix);
    }
    public static KafkaSink<String> sink(PipelineConfig c, KafkaRecordSerializationSchema<String> serializer, String suffix) {
        return KafkaSink.<String>builder().setBootstrapServers(c.get("kafka.bootstrap.servers"))
                .setRecordSerializer(serializer).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(c.get("kafka.transaction.prefix") + "-" + suffix + "-")
                .setProperty("transaction.timeout.ms", c.get("kafka.transaction.timeout.ms")).build();
    }
}
