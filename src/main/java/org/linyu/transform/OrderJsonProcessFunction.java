package org.linyu.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.linyu.map.OrderDetailRealTime;
import org.linyu.util.DirtyOutputTags;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class OrderJsonProcessFunction
        extends ProcessFunction<String, OrderDetailRealTime> {

    /**
     * JSON解析失败的脏数据。
     */

    private transient ObjectMapper objectMapper;

    // json 计算算子
    @Override
    public void processElement(
            String s,
            Context context,
            Collector<OrderDetailRealTime> collector) {
        try {

            OrderDetailRealTime order =
                    objectMapper.readValue(
                            s,
                            OrderDetailRealTime.class
                    );
            if (isBlank(order.orderId)) {
                throw new IllegalArgumentException(
                        "order_id 为空"
                );
            }
            if (isBlank(order.orderStatus)) {
                throw new IllegalArgumentException(
                        "orderState_id 为空"
                );
            }
            collector.collect(order);

        } catch (Exception e) {
            context.output(
                    DirtyOutputTags.JSON_DIRTY_TAG,
                    "原始数据"+ s
                    + ",错误原因："
                    + e.getMessage()
            );

        }

    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper = new ObjectMapper();
    }
}
