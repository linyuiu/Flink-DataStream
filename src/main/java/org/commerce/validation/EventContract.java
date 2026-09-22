package org.commerce.validation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.commerce.common.Contract;
import org.commerce.common.Json;
import org.commerce.model.TradeEvent;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** 入站校验只判断事件契约；真实渠道是否支付成功仍由业务服务负责。 */
public final class EventContract {
    private static final ObjectMapper READER =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private EventContract() {}

    public static TradeEvent parse(String raw) {
        try {
            // 先检查JSON类型，禁止浮点截断、字符串转数字、null或缺失字段自动填0。
            JsonNode document = READER.readTree(raw);
            validateJsonTypes(document);
            TradeEvent event = READER.treeToValue(document, TradeEvent.class);
            validate(event);
            return event;
        } catch (IOException exception) {
            throw new IllegalArgumentException("Invalid event JSON", exception);
        }
    }

    private static void validateJsonTypes(JsonNode document) {
        require(document != null && document.isObject(), "Expected event JSON object");
        for (String field : new String[] {"schemaVersion", "occurredAt", "paidAt", "goodsCent"}) {
            Json.number(document, field);
        }
        for (String field :
                new String[] {
                    "eventId",
                    "eventType",
                    "businessId",
                    "orderId",
                    "checkoutId",
                    "userId",
                    "shopId",
                    "provinceId",
                    "cityId",
                    "districtId",
                    "currency"
                }) {
            Json.text(document, field);
        }
        JsonNode lines = document.path("lines");
        require(lines.isArray(), "Expected order lines array");
        for (JsonNode line : lines) {
            require(line.isObject(), "Expected order line object");
            Json.number(line, "amountCent");
            Json.number(line, "quantity");
            for (String field :
                    new String[] {"orderLineId", "productId", "skuId", "categoryId", "brandId"}) {
                Json.text(line, field);
            }
        }
    }

    public static void validate(TradeEvent event) {
        validateHeader(event);
        validatePaymentTime(event);
        validateLineAllocations(event);
    }

    private static void validateHeader(TradeEvent event) {
        require(event != null && event.schemaVersion == 1, "Unsupported event schema");
        require(
                event.eventType != null && Contract.TYPES.contains(event.eventType),
                "Unknown event type");
        for (String id :
                new String[] {
                    event.eventId,
                    event.businessId,
                    event.orderId,
                    event.checkoutId,
                    event.userId,
                    event.shopId,
                    event.provinceId,
                    event.cityId,
                    event.districtId
                }) {
            identifier(id);
        }
        require("CNY".equals(event.currency), "Only CNY supported");
        require(
                event.occurredAt > 0
                        && event.goodsCent > 0
                        && event.lines != null
                        && !event.lines.isEmpty()
                        && event.lines.size() <= 200,
                "Invalid time/amount/line count");
    }

    private static void validatePaymentTime(TradeEvent event) {
        boolean referencesPayment =
                event.eventType.equals("PAYMENT_SUCCEEDED")
                        || event.eventType.equals("REFUND_SUCCEEDED");
        if (referencesPayment) {
            require(
                    event.paidAt > 0 && event.paidAt <= event.occurredAt,
                    "Invalid payment reference time");
        } else {
            require(event.paidAt == 0, "Invalid payment reference time");
        }
        if (event.eventType.equals("PAYMENT_SUCCEEDED")) {
            require(event.paidAt == event.occurredAt, "Payment times disagree");
        }
    }

    private static void validateLineAllocations(TradeEvent event) {
        long allocatedCent = 0;
        Set<String> orderLineIds = new HashSet<>();
        boolean refund = event.eventType.equals("REFUND_SUCCEEDED");
        for (TradeEvent.Line line : event.lines) {
            require(line != null, "Null line");
            for (String id :
                    new String[] {
                        line.orderLineId, line.productId, line.skuId, line.categoryId, line.brandId
                    }) {
                identifier(id);
            }
            require(orderLineIds.add(line.orderLineId), "Duplicate line within event");
            require(line.amountCent > 0, "Nonpositive line amount");
            // 退款金额与退货数量是两回事，本契约没有退货件数事实。
            require(
                    refund ? line.quantity == 0 : line.quantity > 0,
                    "Refund amount does not imply returned quantity");
            allocatedCent = Math.addExact(allocatedCent, line.amountCent);
        }
        require(allocatedCent == event.goodsCent, "Event header/allocation mismatch");
    }

    public static void identifier(String value) {
        require(value != null && value.matches("[A-Za-z0-9_:.\\-]{1,100}"), "Invalid identifier");
    }

    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
