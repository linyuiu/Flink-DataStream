package org.commerce.function;

import org.apache.flink.util.OutputTag;
import org.commerce.common.Json;

public final class Outputs {
    private Outputs() {}

    public static final OutputTag<String> QUALITY = new OutputTag<String>("commerce-quality-v1") {};
    public static final OutputTag<String> REPAIR = new OutputTag<String>("commerce-repair-v1") {};

    /** 补充产生异常的作业，便于共享 Topic 中的问题分派和定向补算；保留原始诊断字段。 */
    public static String forJob(String record, String job) {
        return Json.object(record).put("job", job).toString();
    }

    public static String record(String stage, String raw, String reason) {
        return Json.MAPPER
                .createObjectNode()
                .put("stage", stage)
                .put("raw", raw)
                .put("reason", reason)
                .put("observed_at", System.currentTimeMillis())
                .toString();
    }
}
