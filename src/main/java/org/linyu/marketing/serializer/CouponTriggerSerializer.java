package org.linyu.marketing.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.linyu.marketing.model.CouponTrigger;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将最终触达事件序列化成 Kafka sink 需要的 JSON 字符串。
 */
public class CouponTriggerSerializer implements MapFunction<CouponTrigger, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public String map(CouponTrigger value) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        return objectMapper.writeValueAsString(value);
    }
}
