package org.linyu.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.linyu.model.DailyGmvRow;

public class DailyGmvRowSerializer implements MapFunction<DailyGmvRow, String> {
    private transient ObjectMapper objectMapper;

    @Override
    public String map(DailyGmvRow dailyGmvRow) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.writeValueAsString(dailyGmvRow);
    }
}
