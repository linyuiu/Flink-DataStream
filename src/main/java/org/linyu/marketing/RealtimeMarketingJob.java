package org.linyu.marketing;

import org.linyu.marketing.function.UserFrequencyControlFunction;
import org.linyu.marketing.function.UserSkuRuleFunction;
import org.linyu.marketing.model.CartEvent;
import org.linyu.marketing.model.PayEvent;
import org.linyu.marketing.model.CouponCandidate;
import org.linyu.marketing.model.CouponTrigger;
import org.linyu.marketing.parser.DebeziumCartEventParser;
import org.linyu.marketing.parser.DebeziumPayEventParser;
import org.linyu.marketing.serializer.CouponTriggerSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 实时营销规则主作业。
 *
 * 输入是购物车和订单 CDC Kafka topic，输出是通过频控后的优惠券触发事件。
 */
public class RealtimeMarketingJob {

    public static void main(String[] args) throws Exception {

        String brokers = "operations-playground-kafka-1:9092";

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 规则计算会保存定时器、去重和频控状态，RocksDB 更适合这种可能增长的 keyed state。
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // Kafka exactly-once sink 依赖 checkpoint 提交事务；没有 checkpoint 就无法保证端到端一致性。
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage(
                "file:///tmp/flink-checkpoints/realtime-marketing"
        );

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        KafkaSource<String> cartSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("ods_cart_add_event")
                .setGroupId("marketing-cart-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("ods_order_item")
                .setGroupId("marketing-order-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<CartEvent> cartStream = env
                .fromSource(
                        cartSource,
                        WatermarkStrategy.noWatermarks(),
                        "cart-cdc-kafka-source"
                )
                .map(new DebeziumCartEventParser())
                .filter(e -> e != null)
                .assignTimestampsAndWatermarks(
                        // CDC 事件时间可能乱序，允许 5 分钟延迟；空闲分区 1 分钟后不再阻塞 watermark。
                        WatermarkStrategy
                                .<CartEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.eventTime)
                                .withIdleness(Duration.ofMinutes(1))
                );

        DataStream<PayEvent> payStream = env
                .fromSource(
                        orderSource,
                        WatermarkStrategy.noWatermarks(),
                        "order-cdc-kafka-source"
                )
                .map(new DebeziumPayEventParser())
                .filter(e -> e != null)
                .assignTimestampsAndWatermarks(
                        // 支付流也使用业务支付时间，保证和加购流在同一时间语义下比较。
                        WatermarkStrategy
                                .<PayEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.payTime)
                                .withIdleness(Duration.ofMinutes(1))
                );

        DataStream<CouponCandidate> candidateStream = cartStream
                // 按用户 + SKU 对齐加购与支付事件：同一个 key 里才能取消“未支付”定时器。
                .keyBy(cart -> cart.userId + "_" + cart.skuId)
                .connect(payStream.keyBy(pay -> pay.userId + "_" + pay.skuId))
                .process(new UserSkuRuleFunction())
                .name("user-sku-rule-function");

        DataStream<CouponTrigger> triggerStream = candidateStream
                // 候选规则转成最终触达前，必须切到用户维度做全局频控。
                .keyBy(candidate -> candidate.userId)
                .process(new UserFrequencyControlFunction())
                .name("user-frequency-control-function");

        KafkaSink<String> triggerSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("coupon_trigger_topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("coupon-trigger-sink-")
                .build();

        triggerStream
                .map(new CouponTriggerSerializer())
                .sinkTo(triggerSink)
                .name("coupon-trigger-kafka-sink");

        env.execute("Realtime Marketing Rule Job");
    }
}
