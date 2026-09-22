package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.commerce.config.CommerceConfig;
import org.commerce.source.Connectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/** 将交易计划中的 Outbox 事实和共用维度目录写入 CDC ODS Topic。 */
final class KafkaMockPublisher implements AutoCloseable {
    private final KafkaProducer<byte[], byte[]> producer;
    private final Connectors.CdcRouter router;

    KafkaMockPublisher(CommerceConfig config, String clientSuffix) {
        Properties properties = new Properties();
        properties.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "commerce-kafka-mock-" + clientSuffix);
        producer = new KafkaProducer<>(properties);
        router = new Connectors.CdcRouter(config);
    }

    /** 使用同一个 Catalog 生成器；等待所有发送确认后再开始发订单。 */
    void publishCatalog() throws Exception {
        List<Future<RecordMetadata>> confirmations = new ArrayList<>();
        for (var table : Catalog.rows().entrySet()) {
            for (ObjectNode row : table.getValue()) {
                confirmations.add(send(table.getKey(), row));
            }
        }
        for (Future<RecordMetadata> confirmation : confirmations) {
            confirmation.get();
        }
    }

    /** 每个事务计划最多一条 Outbox 事件；支付/退款失败的计划没有成功事实。 */
    long publishOrder(List<TransactionPlan> plans) throws Exception {
        long published = 0;
        for (TransactionPlan plan : plans) {
            if (plan.event != null) {
                send("trade_outbox", MockCdcRecord.outboxRow(plan.event)).get();
                published++;
            }
        }
        return published;
    }

    private Future<RecordMetadata> send(String table, ObjectNode row) {
        String envelope = MockCdcRecord.insert(table, row);
        return producer.send(router.serialize(envelope, null, null));
    }

    @Override
    public void close() {
        producer.close();
    }
}
