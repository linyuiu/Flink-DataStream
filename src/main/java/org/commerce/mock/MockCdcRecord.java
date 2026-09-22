package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.commerce.common.Json;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

/** 构造与当前 CDC 消费者契约一致的插入消息；不伪造 MySQL binlog 位点。 */
final class MockCdcRecord {
    private MockCdcRecord() {}

    static ObjectNode outboxRow(TradeEvent event) {
        EventContract.validate(event);
        return Json.MAPPER
                .createObjectNode()
                .put("id", event.eventId)
                .put("order_id", event.orderId)
                .put("event_type", event.eventType)
                .put("business_id", event.businessId)
                .put("occurred_at", event.occurredAt)
                .put("payload_json", Json.write(event));
    }

    static String insert(String table, ObjectNode row) {
        ObjectNode envelope = Json.MAPPER.createObjectNode();
        envelope.putObject("source").put("table", table);
        envelope.put("op", "c");
        envelope.set("after", row.deepCopy());
        return envelope.toString();
    }
}
