package org.linyureal.parser;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.*;
import org.linyureal.model.dimension.DimensionRow;
import java.util.*;

public class DimensionParseFunction extends ProcessFunction<String,DimensionRow> {
    public static final List<String> TABLES=Tables.DIMENSIONS;
    @Override public void processElement(String raw,Context ctx,Collector<DimensionRow> out) throws Exception {
        DimensionRow d;
        try {
            JsonNode p=Json.MAPPER.readTree(raw);
            if(p==null || p.isNull()) return;
            if(p.has("payload")) p=p.get("payload");
            String table=p.has("table")?Json.text(p,"table"):Json.text(p.path("source"),"table");
            if(!TABLES.contains(table) || !Arrays.asList("c","u","r").contains(Json.text(p,"op")))
                throw new IllegalArgumentException("Unsupported dimension operation/table");
            JsonNode r=p.path("after");
            d=new DimensionRow();
            d.dimension_type=table.equals("dim_region")?Json.text(r,"region_level"):table.substring(4).toUpperCase(Locale.ROOT);
            if(table.equals("dim_region") && !Arrays.asList("PROVINCE","CITY","DISTRICT").contains(d.dimension_type))
                throw new IllegalArgumentException("Unknown region level");
            d.dimension_id=Json.text(r,"id"); d.display_name=Json.text(r,"name");
            d.parent_id=Json.text(r,"parent_id"); d.version=Json.number(r,"version");
            if(d.version<=0) throw new IllegalArgumentException("Invalid version");
        } catch(Exception e) { ctx.output(Dirty.TAG,Dirty.record("dimension-parse",raw,e.getMessage())); return; }
        out.collect(d);
    }
}
