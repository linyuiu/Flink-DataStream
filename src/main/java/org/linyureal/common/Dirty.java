package org.linyureal.common;

import org.apache.flink.util.OutputTag;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class Dirty {
    public static final OutputTag<String> TAG = new OutputTag<String>("linyureal-dirty-v1") {};
    private Dirty() {}
    public static String record(String stage,String raw,String reason) {
        ObjectNode n=Json.MAPPER.createObjectNode();
        n.put("stage",stage); n.put("raw",raw); n.put("reason",reason);
        n.put("observed_at_ms",System.currentTimeMillis()); return n.toString();
    }
}
