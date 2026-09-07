package org.linyureal.function.metric;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.linyureal.common.*;
import org.linyureal.model.dimension.DimensionRow;

public class DimensionVersionFunction extends KeyedProcessFunction<String,DimensionRow,String> {
    private transient ValueState<DimensionRow> state;
    @Override public void open(OpenContext ctx) {
        state=getRuntimeContext().getState(new ValueStateDescriptor<>("dimension-version-v1",DimensionRow.class));
    }
    @Override public void processElement(DimensionRow d,Context ctx,Collector<String> out) throws Exception {
        DimensionRow old=state.value();
        if(old!=null && d.version<=old.version) {
            if(d.version==old.version && !Json.write(d).equals(Json.write(old)))
                ctx.output(Dirty.TAG,Dirty.record("dimension-conflict",Json.write(d),"Same version changed"));
            return;
        }
        state.update(d); out.collect(Json.write(d));
    }
}
