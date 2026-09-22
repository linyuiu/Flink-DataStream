package org.commerce.function;

import static org.commerce.validation.EventContract.require;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.commerce.common.Json;
import org.commerce.model.TradeEvent;
import org.commerce.validation.EventContract;

import java.util.Comparator;

/** 解析不可变Outbox事实。清理删除只回收源库记录；业务冲正必须发布独立的业务事件。 */
public class OutboxParseFunction extends ProcessFunction<String, String> {
    private transient Counter cleanupDeletes;
    private transient Counter invalidEvents;

    @Override
    public void open(OpenContext context) {
        cleanupDeletes = getRuntimeContext().getMetricGroup().counter("outbox_cleanup_deletes");
        invalidEvents = getRuntimeContext().getMetricGroup().counter("invalid_outbox_events");
    }

    /** 返回规范化事实；源库归档清理产生的DELETE返回null，不撤回已发布事实。 */
    public static String parse(String raw) {
        JsonNode envelope = Json.object(raw);
        if (envelope.has("payload")) {
            envelope = envelope.get("payload");
        }
        require(
                "trade_outbox".equals(Json.text(envelope.path("source"), "table")),
                "Not a trade outbox event");
        String operation = Json.text(envelope, "op");
        if ("d".equals(operation)) {
            return null;
        }
        require(java.util.List.of("c", "r").contains(operation), "Outbox facts cannot be updated");
        JsonNode after = envelope.path("after");
        TradeEvent event = EventContract.parse(Json.text(after, "payload_json"));
        require(event.eventId.equals(Json.text(after, "id")), "Outbox/event ID mismatch");
        require(
                event.orderId.equals(Json.text(after, "order_id"))
                        && event.eventType.equals(Json.text(after, "event_type")),
                "Outbox routing mismatch");
        require(
                event.businessId.equals(Json.text(after, "business_id")),
                "Outbox business ID mismatch");
        require(
                event.occurredAt == Json.number(after, "occurred_at"),
                "Outbox event time mismatch");
        // 先统一明细顺序，再生成去重指纹，避免仅明细排列变化被误判为内容冲突。
        event.lines.sort(Comparator.comparing(line -> line.orderLineId));
        return Json.write(event);
    }

    @Override
    public void processElement(String raw, Context context, Collector<String> output) {
        String event;
        try {
            event = parse(raw);
        } catch (RuntimeException exception) {
            invalidEvents.inc();
            context.output(
                    Outputs.QUALITY,
                    Outputs.record("outbox-contract", raw, exception.getMessage()));
            return;
        }
        if (event == null) {
            cleanupDeletes.inc();
            return;
        }
        output.collect(event);
    }
}
