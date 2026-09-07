package org.linyureal.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.*;
import org.linyureal.config.AppConfig;

public final class KafkaSinks {
    private KafkaSinks() {}
    public static KafkaSink<String> create(AppConfig c,String topic,String suffix) {
        return KafkaSink.<String>builder().setBootstrapServers(c.get("kafka.bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder().setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(c.get("kafka.transaction.prefix")+"-"+suffix+"-")
                .setProperty("transaction.timeout.ms",c.get("kafka.transaction.timeout.ms")).build();
    }
}
