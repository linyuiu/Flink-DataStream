package org.linyu.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.linyu.config.DorisSinkConfig;

import java.util.Properties;

public class DorisSinkFactory {

    private DorisSinkFactory() {

    }

    public static DorisSink<String> create(DorisSinkConfig config) {

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setPassword(config.getPassword())
                        .setUsername(config.getUsername())
                        .setFenodes(config.getFeNodes())
                        .setTableIdentifier(config.getTableIdentifier())
                        .build();

        Properties properties =
                new Properties();

        properties.setProperty(
                "format",
                "json"
        );

        properties.setProperty(
                "read_json_by_line",
                "true"
        );
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix(
                        config.getLabelPrefix()
                )
                .enable2PC()
                .setDeletable(false)
                .setStreamLoadProp(properties)
                .build();


        return DorisSink.<String>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(
                        DorisReadOptions.defaults()
                )
                .setDorisExecutionOptions(
                        executionOptions
                )
                .setSerializer(
                        new SimpleStringSerializer()
                ).build();

    }
}
