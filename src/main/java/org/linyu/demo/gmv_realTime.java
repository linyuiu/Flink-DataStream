package org.linyu.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.tools.cdc.ParsingProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.linyu.config.ConfigUtil;
import org.linyu.map.OrderDetail;

import java.io.Serializable;
import java.math.BigDecimal;


public class gmv_realTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(
                ConfigUtil.getLong("flink.checkpoint.interval", 10_000L),
                CheckpointingMode.EXACTLY_ONCE
        );

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointTimeout(60_000L);

        checkpointConfig.setMinPauseBetweenCheckpoints(5_000L);

        checkpointConfig.setMaxConcurrentCheckpoints(1);

        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup
                        .RETAIN_ON_CANCELLATION
        );

        String property = System.getProperty(
                "checkpoint.dir",
                "file:///tmp/flink/flink-checkpoints/simple-gmv"
        );

        Configuration flinkConfiguration = new Configuration();

        flinkConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                property
        );

        env.configure(flinkConfiguration);





        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.9.53:9092")
                .setTopics("dwd_order_detail")
                .setGroupId("transformationDemo")
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(
                                OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();



        DataStreamSource<String> kafkaSource =
                env.fromSource(
                        source,
                                WatermarkStrategy.noWatermarks(),
                                "dwd-order-detail-source"
                );
        SingleOutputStreamOperator<String> streamOperator = kafkaSource
                .map(new JsonToOrderMapFunction())
                .name("parse-order-json")
                .uid("parse-order-json");





    }
    public static class JsonToOrderMapFunction
            extends RichMapFunction<String, OrderDetail> {
        private transient ObjectMapper objectMapper;

        @Override
        public void open(OpenContext s) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public OrderDetail map(String  json) throws Exception {
            return objectMapper.readValue(
                    json,
                    OrderDetail.class
            );
        }
    }

    public static class DailyGmvReduceFunction
            implements ReduceFunction<DailyGmv> {


        @Override
        public DailyGmv reduce(DailyGmv dailyGmv, DailyGmv t1) throws Exception {
            return new DailyGmv();
        }
    }

    public static class  DailyGmv implements Serializable{
        public String bizDate;
        public BigDecimal gmv;
        public String updateTime;

        public DailyGmv(){}

        public DailyGmv(
                String bizDate,
                BigDecimal gmv,
                String updateTime) {

            this.bizDate = bizDate;
            this.gmv = gmv;
            this.updateTime = updateTime;
        }

    }

}



