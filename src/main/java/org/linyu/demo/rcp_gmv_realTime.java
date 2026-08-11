package org.linyu.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


import org.linyu.config.ConfigUtil;
import org.linyu.map.DailyGmvRealTime;
import org.linyu.map.GmvDeltaRealTime;
import org.linyu.map.OrderDetailRealTime;
import org.linyu.sink.DorisSinkRealTime;
import org.linyu.transform.DailyGmvAccumulatorFunction;
import org.linyu.transform.DailyGmvJsonProcessFunction;
import org.linyu.transform.OrderContributionProcessFunction;
import org.linyu.transform.OrderJsonProcessFunction;



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
     * 业务字段非法的数据。
     */



    public static void main(String[] args) throws Exception {


        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        //配置 checkpoint
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

        env.configure(configuration);




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

        SingleOutputStreamOperator<OrderDetailRealTime> orderStream = kafkaJsonSource
                .process(
                        new OrderJsonProcessFunction()
                )
                .name("parse-order-json")
                .uid("advanced-parse-order-json");

        SideOutputDataStream<String> jsonDirtyStream = orderStream.getSideOutput(
                OrderJsonProcessFunction.JSON_DIRTY_TAG
        );

        /*
         * 3. 按照 order_id 分组。
         *
         * 相同 order_id 的所有状态更新，
         * 必须进入同一个 KeyedProcessFunction 实例。
         */
        SingleOutputStreamOperator<GmvDeltaRealTime> deltaStream = orderStream
                .keyBy(order -> order.orderId)

                /*
                 * 保存订单上一次状态，
                 * 计算本次状态相对于上一次状态的变化量。
                 */
                .process(new OrderContributionProcessFunction()
                )
                .name("calculate-order-gmv-delta")
                .uid("calculate-order-gmv-delta");

        SideOutputDataStream<String> businessDirtyStream = deltaStream.getSideOutput(
                OrderJsonProcessFunction.BUSINESS_DIRTY_TAG
        );

        /*
         * 4. 按业务日期累计变化量。
         *
         * 输入示例：
         *
         * 2026-07-31 +100
         * 2026-07-31  -30
         * 2026-07-31  -70
         *
         * 最终：
         * 2026-07-31 = 0
         */
        SingleOutputStreamOperator<DailyGmvRealTime> dailyGmvStream = deltaStream
                .keyBy(delta -> delta.bizDate)
                .process(
                        new DailyGmvAccumulatorFunction()
                )
                .name("accumulate-daily-gmv")
                .uid("");

        /*5 转化为  doris json*/

        SingleOutputStreamOperator<String> dorisJsonStream = dailyGmvStream
                .process(
                        new DailyGmvJsonProcessFunction()
                )
                .name("daily-gmv-to-json")
                .uid("daily-gmv-to-json");
        /*6 写入 doris*/

        dorisJsonStream.sinkTo(
                DorisSinkRealTime.buildDorisSink(
                        ConfigUtil.getString("doris.password"),
                        ConfigUtil.getString("doris.username"),
                        ConfigUtil.getString("doris.fenodes"),
                        ConfigUtil.getString("doris.table.identifier.dailyGmvRealTime")))
                .name("advanced-doris-gmv-sink")
                .uid("advanced-doris-gmv-sink");

        /*
        * 这里只是演示
        * 正式环境应该把脏数据写入
        * 1. kafka dirty topic
        * 2. Doris 脏数据表
        * 3. 日志系统
        *
        * */
        jsonDirtyStream.union(businessDirtyStream).print("DIRTY");

        env.execute("Advanced Realtime Daily GMV");



    }
}
