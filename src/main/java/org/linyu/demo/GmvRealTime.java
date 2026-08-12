package org.linyu.demo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;


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

import java.time.LocalDateTime;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.time.ZoneId;

public class GmvRealTime {

    private static final ZoneId BUSINESS_ZONE =
            ZoneId.of("Asia/Shanghai");

    private static final DateTimeFormatter OUTPUT_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        //创建 streaming 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置 checkpoint 时间以及语义
        env.enableCheckpointing(
                ConfigUtil.getLong("flink.checkpoint.interval", 10_000L),
                CheckpointingMode.EXACTLY_ONCE
        );

        //设置 checkpoint 配置
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

        flinkConfiguration.set(CheckpointingOptions.CHECKPOINT_STORAGE,
                "filesystem");

        flinkConfiguration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                property
        );

        env.configure(flinkConfiguration);



        /*
        * 创建 kafka source
        *  json 转化为 string
        * */

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.9.53:9092")
                .setTopics("dwd_order_detail")
                .setGroupId("simple-realtime-gmv-job")
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(
                                OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();



        /*
        *
        * kafka 创建 dataStream
        * */

        DataStreamSource<String> kafkaSource =
                env.fromSource(
                        source,
                                WatermarkStrategy.noWatermarks(),
                                "dwd-order-detail-source"
                );
                        kafkaSource
                                .uid("kafka-order-source")
                                .name("kafka-order-source");

        SingleOutputStreamOperator<OrderDetail> streamOperator = kafkaSource
                .map(new JsonToOrderMapFunction())
                .name("parse-order-json")
                .uid("parse-order-json");

        SingleOutputStreamOperator<OrderDetail> paidOrderStream =
                streamOperator
                        .filter(order ->
                                order != null
                                        && order.orderId != null
                                        && order.payTime != null
                                        && order.payAmount != null
                                        && order.payAmount
                                        .compareTo(BigDecimal.ZERO) >= 0
                                        && "PAID".equalsIgnoreCase(
                                        order.orderStatus
                                )
                        )
                        .name("filter-paid-orders")
                        .uid("filter-paid-orders");

        SingleOutputStreamOperator<DailyGmv> orderGmvStream = paidOrderStream
                .map(order -> new DailyGmv(
                        extractDate(order.payTime),
                        order.payAmount,
                        currentTime()
                ))
                .name("order-to-gmv")
                .uid("order-to-gmv");

        SingleOutputStreamOperator<DailyGmv> runningGmvStream = orderGmvStream
                .keyBy(value -> value.bizDate)
                .reduce(new DailyGmvReduceFunction())
                .name("daily-running-gmv")
                .uid("daily-running-gmv");

        SingleOutputStreamOperator<String> dorisJsonStream = runningGmvStream
                .map(new DailyGmvToJsonFunction())
                .name("gmv-to-doris-json")
                .uid("gmv-to-doris-json");

        dorisJsonStream
                .sinkTo(buildDorisSink())
                .name("doris-gmv-sink")
                .uid("doris-gmv-sink");

        env.execute("Simple Realtime Daily GMV");


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
            return new DailyGmv(
                    dailyGmv.bizDate,
                    dailyGmv.gmv.add(t1.gmv),
                    currentTime()
            );
        }
    }

    public static class DailyGmvToJsonFunction
            extends RichMapFunction<DailyGmv, String> {
        private  transient ObjectMapper objectMapper;


        @Override
        public String map(DailyGmv dailyGmv) throws Exception {
            ObjectNode jsonNodes = objectMapper.createObjectNode();
            jsonNodes.put("biz_date", dailyGmv.bizDate);
            jsonNodes.put("gmv", dailyGmv.gmv);
            jsonNodes.put("update_time", dailyGmv.updateTime);
            return jsonNodes.toString();
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            objectMapper = new ObjectMapper();
        }
    }

    private  static DorisSink<String> buildDorisSink(){
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("192.168.9.53:38030")
                .setTableIdentifier("realtime_ads.gmv_realtime_today")
                .setUsername("root")
                .setPassword("Linyu@2026")
                .build();
        Properties properties = new Properties();
        properties.setProperty(
                "format",
                "json"
        );
        properties.setProperty(
                "read_json_by_line",
                "true"
        );

        DorisExecutionOptions build = DorisExecutionOptions.builder()
                .setLabelPrefix("simple-realtime-gmv-v1")
                .setDeletable(false)
                .setStreamLoadProp(
                        properties
                )
                .build();

        DorisSink.Builder<String> builder =
                DorisSink.builder();

        builder
                .setDorisReadOptions(
                        DorisReadOptions.builder().build()
                )
                .setDorisExecutionOptions(
                        build
                )
                .setSerializer(
                        new SimpleStringSerializer()
                )
                .setDorisOptions(
                        dorisOptions
                );

        return builder.build();

    }
    private static String extractDate(String dateTime) {

        if (dateTime == null || dateTime.length() < 10) {
            throw new IllegalArgumentException(
                    "pay_time格式错误：" + dateTime
            );
        }

        return dateTime.substring(0, 10);
    }

    private static String currentTime() {
        return LocalDateTime
                .now(BUSINESS_ZONE)
                .format(OUTPUT_TIME_FORMATTER);
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderDetail implements Serializable {

        @JsonProperty("order_id")
        public String orderId;

        @JsonProperty("user_id")
        public String userId;

        @JsonProperty("sku_id")
        public String skuId;

        @JsonProperty("pay_amount")
        public BigDecimal payAmount;

        @JsonProperty("refund_amount")
        public BigDecimal refundAmount;

        @JsonProperty("order_status")
        public String orderStatus;

        @JsonProperty("create_time")
        public String createTime;

        @JsonProperty("pay_time")
        public String payTime;

        @JsonProperty("dt")
        public String dt;


        public OrderDetail() {
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



