package org.linyu.source;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.linyu.config.ConfigUtil;

import java.util.Locale;

public class KafkaSourceConfig {


    public static OffsetsInitializer buildStartingOffsets() {
        String mode = ConfigUtil.getString(
                "kafka.starting.offsets",
                "committed"
        );

        switch (mode.toLowerCase(Locale.ROOT)) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            case "committed":
                return OffsetsInitializer.committedOffsets(
                        OffsetResetStrategy.EARLIEST
                );
            default:
                throw new IllegalArgumentException(
                        "不支持的 Kafka 启动模式" + mode
                );
        }

    }

}
