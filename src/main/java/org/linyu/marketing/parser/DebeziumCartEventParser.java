package org.linyu.marketing.parser;

import org.linyu.marketing.model.CartEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 将 Debezium 输出的购物车 CDC JSON 转成内部 CartEvent。
 */
public class DebeziumCartEventParser implements MapFunction<String, CartEvent> {

    private transient ObjectMapper mapper;

    @Override
    public CartEvent map(String value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        JsonNode root = mapper.readTree(value);

        String op = root.path("op").asText();

        // Debezium op: c=create, u=update, d=delete, r=snapshot read；删除事件没有加购语义。
        if ("d".equals(op)) {
            return null;
        }

        JsonNode after = root.path("after");
        if (after.isMissingNode() || after.isNull()) {
            return null;
        }

        CartEvent event = new CartEvent();
        event.eventId = after.path("event_id").asText();
        event.userId = after.path("user_id").asLong();
        event.skuId = after.path("sku_id").asLong();
        event.spuId = after.path("spu_id").asLong();
        event.quantity = after.path("quantity").asInt(1);
        event.eventTime = parseDateTime(after.path("event_time").asText());

        return event;
    }

    private long parseDateTime(String dateTime) {
        // 源表存的是无时区字符串，按业务时区解释后再转成 epoch millis。
        LocalDateTime ldt = LocalDateTime.parse(
                dateTime,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        );

        return ldt.atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant()
                .toEpochMilli();
    }
}
