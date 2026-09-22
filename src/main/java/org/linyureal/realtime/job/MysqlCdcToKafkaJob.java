package org.linyureal.realtime.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.linyureal.realtime.config.PipelineConfig;
import org.linyureal.realtime.source.*;
import org.linyureal.realtime.sink.CdcTopicSerializer;

/** Real Flink CDC source: initial snapshot followed by MySQL binlog, no Kafka Connect service. */
public final class MysqlCdcToKafkaJob {
    public static void main(String[] args) throws Exception {
        PipelineConfig c = PipelineConfig.load(args);
        StreamExecutionEnvironment env = Environments.create(c, "cdc");
        env.fromSource(MysqlCdcSourceFactory.create(c), WatermarkStrategy.noWatermarks(), "mysql-cdc")
                .uid("rttrade-mysql-cdc-v1")
                .sinkTo(KafkaIo.sink(c, new CdcTopicSerializer(c), "cdc")).uid("rttrade-ods-kafka-v1");
        env.execute("RTTrade MySQL CDC to Kafka");
    }
}
