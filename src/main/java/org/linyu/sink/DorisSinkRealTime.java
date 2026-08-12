package org.linyu.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Properties;

public class DorisSinkRealTime {

    public static DorisSink<String> buildDorisSink(String password,
                                                   String username,
                                                   String serverPort,
                                                   String tableName,
                                                   String tablePrefix) {

        DorisOptions dorisOptions = DorisOptions.builder()
                .setPassword(password)
                .setUsername(username)
                .setFenodes(serverPort)
                .setTableIdentifier(tableName)
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
                .setLabelPrefix(tablePrefix)
                .setDeletable(false)
                .setStreamLoadProp(properties)
                .build();

        DorisSink.Builder<String> builder = DorisSink.<String>builder();

        builder.setDorisReadOptions(
                        DorisReadOptions.builder().build()
                )
                .setDorisExecutionOptions(
                        executionOptions
                )
                .setSerializer(new SimpleStringSerializer()
                )
                .setDorisOptions(
                        dorisOptions
                );
        return builder.build();

    }
}
