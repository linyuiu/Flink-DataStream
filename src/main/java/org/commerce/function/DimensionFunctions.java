package org.commerce.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.*;
import org.apache.flink.util.Collector;
import org.commerce.common.*;

/** 维度路径只维护最新显示信息；交易金额使用事实中的历史维度 ID，不在这里重新归属。 */
public final class DimensionFunctions {
    private DimensionFunctions() {}

    public static String key(String json) {
        var row = Json.object(json);
        return Json.text(row, "dimension_type") + ":" + Json.text(row, "dimension_id");
    }

    public static class Parse extends ProcessFunction<String, String> {
        @Override
        public void processElement(String raw, Context context, Collector<String> output) {
            String json;
            try {
                var envelope = Json.object(raw);
                String table = Json.text(envelope.path("source"), "table");
                if (!Contract.CDC_TABLES.contains(table)
                        || !table.startsWith("dim_")
                        || !java.util.List.of("r", "c", "u").contains(Json.text(envelope, "op")))
                    throw new IllegalArgumentException("Unsupported dimension table/operation");
                var row = envelope.path("after");
                String dimensionType =
                        table.equals("dim_region")
                                ? Json.text(row, "region_level")
                                : table.substring(4).toUpperCase(java.util.Locale.ROOT);
                if (table.equals("dim_region")
                        && !java.util.List.of("PROVINCE", "CITY", "DISTRICT")
                                .contains(dimensionType))
                    throw new IllegalArgumentException("Invalid region level");
                long version = Json.number(row, "version");
                if (version <= 0) throw new IllegalArgumentException("Invalid dimension version");
                json =
                        Json.MAPPER
                                .createObjectNode()
                                .put("dimension_type", dimensionType)
                                .put("dimension_id", Json.text(row, "id"))
                                .put("name", Json.text(row, "name"))
                                .put("parent_id", Json.text(row, "parent_id"))
                                .put("version", version)
                                .put("attributes_json", row.toString())
                                .toString();
            } catch (RuntimeException exception) {
                context.output(
                        Outputs.QUALITY, Outputs.record("dimension", raw, exception.getMessage()));
                return;
            }
            output.collect(json);
        }
    }

    public static class Latest extends KeyedProcessFunction<String, String, String> {
        private transient ValueState<String> state;

        @Override
        public void open(OpenContext context) {
            state =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("dimension-v1", String.class));
        }

        @Override
        public void processElement(String json, Context context, Collector<String> output)
                throws Exception {
            String previous = state.value();
            long version = Json.number(Json.object(json), "version");
            if (previous != null) {
                long previousVersion = Json.number(Json.object(previous), "version");
                if (version < previousVersion) {
                    return;
                }
                if (version == previousVersion) {
                    if (!Json.object(previous).equals(Json.object(json))) {
                        context.output(
                                Outputs.QUALITY,
                                Outputs.record("dimension-conflict", json, "Same version changed"));
                    }
                    return;
                }
            }
            state.update(json);
            output.collect(json);
        }
    }
}
