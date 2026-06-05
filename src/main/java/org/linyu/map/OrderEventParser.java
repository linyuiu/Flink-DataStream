package org.linyu.map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;

/**
 * 将 Kafka 中 dwd_order_detail 的 JSON 字符串解析成 OrderEvent。
 */
public class OrderEventParser implements MapFunction<String, OrderEvent> {

    private transient ObjectMapper mapper;

    @Override
    public OrderEvent map(String value) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        JsonNode root = mapper.readTree(value);
        String orderId = text(root, "order_id");
        if (!OrderEvent.hasText(orderId)) {
            return null;
        }

        OrderEvent event = new OrderEvent();
        event.orderId = orderId;
        event.userId = text(root, "user_id");
        event.payAmount = decimal(root, "pay_amount");
        event.refundAmount = decimal(root, "refund_amount");
        event.orderStatus = text(root, "order_status");
        event.payTime = text(root, "pay_time");
        event.dt = text(root, "dt");
        return event;
    }

    private static String text(JsonNode root, String field) {
        JsonNode node = root.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return "";
        }
        return node.asText("");
    }

    private static BigDecimal decimal(JsonNode root, String field) {
        String value = text(root, field);
        return OrderEvent.hasText(value) ? new BigDecimal(value) : BigDecimal.ZERO;
    }
}
