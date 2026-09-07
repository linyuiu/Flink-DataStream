package org.linyureal.mock.writer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.linyureal.common.Json;
import org.linyureal.config.AppConfig;
import org.linyureal.model.cdc.Change;
import java.util.*;

public final class KafkaTradeWriter implements TradeWriter {
    private final KafkaProducer<String,String> producer;
    private final AppConfig config;
    public KafkaTradeWriter(AppConfig c) {
        config=c;
        Properties p=new Properties(); p.put("bootstrap.servers",c.get("kafka.bootstrap.servers"));
        p.put("key.serializer",StringSerializer.class.getName()); p.put("value.serializer",StringSerializer.class.getName());
        p.put("acks","all"); p.put("enable.idempotence","true");
        producer=new KafkaProducer<>(p);
    }
    @Override public void write(List<Change> changes) throws Exception {
        for(Change c:changes) producer.send(new ProducerRecord<>(config.topic(c.table),
                Json.text(c.after,"id"),Json.write(c))).get();
    }
    @Override public void close() { producer.close(); }
}
