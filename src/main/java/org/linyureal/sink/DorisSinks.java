package org.linyureal.sink;

import org.apache.doris.flink.cfg.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.linyureal.config.AppConfig;
import java.util.Properties;

public final class DorisSinks {
    private DorisSinks() {}
    public static DorisSink<String> create(AppConfig c,String tableKey,String suffix) {
        Properties props=new Properties();
        props.setProperty("format","json"); props.setProperty("read_json_by_line","true");
        props.setProperty("strict_mode","true"); props.setProperty("max_filter_ratio","0");
        return DorisSink.<String>builder()
                .setDorisOptions(DorisOptions.builder().setFenodes(c.get("doris.fenodes"))
                        .setUsername(c.get("doris.username")).setPassword(c.secret("LINYUREAL_DORIS_PASSWORD"))
                        .setTableIdentifier(c.get(tableKey)).build())
                .setDorisReadOptions(DorisReadOptions.defaults())
                .setDorisExecutionOptions(DorisExecutionOptions.builder().enable2PC().setDeletable(false)
                        .setLabelPrefix(c.get("doris.label.prefix")+"_"+suffix).setStreamLoadProp(props).build())
                .setSerializer(new SimpleStringSerializer()).build();
    }
}
