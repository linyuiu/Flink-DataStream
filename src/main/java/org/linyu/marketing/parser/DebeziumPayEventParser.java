package org.linyu.marketing.parser;

import org.linyu.marketing.model.PayEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 将 Debezium 输出的订单 CDC JSON 转成内部 PayEvent。
 */
public class DebeziumPayEventParser implements MapFunction<String, PayEvent> {

    private transient ObjectMapper mapper;

    @Override
    public PayEvent map(String value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        JsonNode root = mapper.readTree(value);

        String op = root.path("op").asText();
        // 删除事件不代表支付成功，直接过滤。
        if ("d".equals(op)) {
            return null;
        }

        JsonNode after = root.path("after");
        if (after.isMissingNode() || after.isNull()) {
            return null;
        }

        String orderStatus = after.path("order_status").asText();

        // 只有支付成功或交易完成状态才会取消“加购未支付”的候选规则。
        if (!"PAID".equals(orderStatus)
                && !"FINISHED".equals(orderStatus)) {
            return null;
        }

        if (after.path("pay_time").isMissingNode()
                || after.path("pay_time").isNull()) {
            return null;
        }

        PayEvent event = new PayEvent();
        event.eventId = "pay_" + after.path("order_id").asText();
        event.orderId = after.path("order_id").asLong();
        event.userId = after.path("user_id").asLong();
        event.skuId = after.path("sku_id").asLong();
        event.payTime = parseDateTime(after.path("pay_time").asText());

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
