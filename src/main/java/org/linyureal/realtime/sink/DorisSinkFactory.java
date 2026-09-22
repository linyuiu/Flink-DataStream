package org.linyureal.realtime.sink;

import org.apache.doris.flink.cfg.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.linyureal.realtime.config.PipelineConfig;
import java.util.Properties;

public final class DorisSinkFactory {
    private DorisSinkFactory() {}
    public static DorisSink<String> create(PipelineConfig c, String tableKey, String suffix) {
        Properties p = new Properties(); p.setProperty("format", "json"); p.setProperty("read_json_by_line", "true");
        p.setProperty("strict_mode", "true"); p.setProperty("max_filter_ratio", "0");
        return DorisSink.<String>builder().setDorisOptions(DorisOptions.builder()
                .setFenodes(c.get("doris.fenodes")).setTableIdentifier(c.get(tableKey))
                .setUsername(c.get("doris.username")).setPassword(c.secret("RTTRADE_DORIS_PASSWORD")).build())
                .setDorisReadOptions(DorisReadOptions.defaults())
                .setDorisExecutionOptions(DorisExecutionOptions.builder().enable2PC().setDeletable(false)
                        .setLabelPrefix(c.get("doris.label.prefix") + "_" + suffix).setStreamLoadProp(p).build())
                .setSerializer(new SimpleStringSerializer()).build();
    }
}
