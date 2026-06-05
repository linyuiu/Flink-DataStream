package org.linyu.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将 GMV 快照序列化成 Doris Stream Load 需要的 JSON 行。
 */
public class DailyGmvResultSerializer implements MapFunction<DailyGmvResult, String> {

    private transient ObjectMapper mapper;

    @Override
    public String map(DailyGmvResult value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper.writeValueAsString(value);
    }
}
