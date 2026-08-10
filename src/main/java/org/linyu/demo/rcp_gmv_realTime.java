package org.linyu.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.linyu.config.ConfigUtil;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class rcp_gmv_realTime {
    /*
    * JSON解析
    ↓
脏数据侧输出
    ↓
keyBy(order_id)
    ↓
保存订单上一次贡献
    ↓
计算本次变化量delta
    ↓
keyBy(biz_date)
    ↓
累计当天GMV
    ↓
每5秒输出一次最新结果
    ↓
Doris
    * */

    /**
     * 是否从 GMV 中扣除退款。

     * true：
     * 净GMV = pay_amount - refund_amount

     * false：
     * 毛GMV = pay_amount
     */
    private static final boolean DEDUCT_REFUNDS = true;

    private static final long OUTPUT_INTERVAL_MS =
            5_000L;

    private static final ZoneId BUSINESS_ZONE =
            ZoneId.of("Asia/Shanghai");

    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );




    /**
     * 业务字段非法的数据。
     */
    private static final OutputTag<String> BUSINESS_DIRTY_TAG =
            new OutputTag<String>("business-dirty-data") {
            };
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置

        env.enableCheckpointing(
                ConfigUtil.getLong("flink.checkpoint.interval", 10_1000L),
                CheckpointingMode.EXACTLY_ONCE
        );

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(30_000L);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup
                        .RETAIN_ON_CANCELLATION);
        checkpointConfig.setMinPauseBetweenCheckpoints(5_000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        String property = System.getProperty(
                "checkpoint.dir",
                "file:///tmp/flink/flink-checkpoints/simple-gmv");

        Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                property
        );


        //添加 kafka source


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(ConfigUtil.getString("kafka.bootstrap.servers"))
                .setTopics(ConfigUtil.getString("kafka.topic.gvm_realtime"))
                .setGroupId("kafka.group.id.dagGmv")
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(
                                OffsetResetStrategy.EARLIEST
                        )
                )
                // 转化为 json
                .setValueOnlyDeserializer(
                        new SimpleStringSchema()
                )
                .build();

        // 连接 kafka source 生成 stream 流
        SingleOutputStreamOperator<String> kafkaJsonSource = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka order Source"
                )
                .uid("connect-kafka-order-source");

        /*
        * 解析 json
        * 1 成功
        * 输出到主流 orderDetail
        *
        * 2 失败
        * 输出到 JSON_DIRTY_TAG
        *
        * */

        kafkaJsonSource.process()

    }
}
