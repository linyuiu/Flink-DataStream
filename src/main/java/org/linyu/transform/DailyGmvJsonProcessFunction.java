package org.linyu.transform;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.linyu.map.DailyGmvRealTime;

/**
 * DailyGmv转换为Doris JSON。
 */
public class DailyGmvJsonProcessFunction
        extends ProcessFunction<DailyGmvRealTime, String> {
    private transient ObjectMapper objectMapper;

    @Override
    public void processElement(DailyGmvRealTime dailyGmvRealTime,
                               Context context,
                               Collector<String> collector) throws Exception {

        ObjectNode json = objectMapper.createObjectNode();

        json.put(
                "biz_date",
                dailyGmvRealTime.bizDate
        );

        json.put(
                "gmv",
                dailyGmvRealTime.gmv);

        json.put("update_time",
                dailyGmvRealTime.updateTime);

        collector.collect(
                json.toString()
        );


    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper = new ObjectMapper();

    }
}
