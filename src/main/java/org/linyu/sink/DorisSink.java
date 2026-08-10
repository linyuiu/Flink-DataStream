package org.linyu.sink;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;

import java.util.Properties;

public class DorisSinkRealTime {

    private static DorisSink<String> buildDorisSink() {

        DorisOptions dorisOptions = DorisOptions.builder()
                .setPassword("")
                .setUsername("")
                .setFenodes("")
                .setTableIdentifier("")
                .build();

        Properties properties =
                new Properties();

        properties.setProperty(
                "format",
                "json"
        );

        properties.setProperty()

    }
}
