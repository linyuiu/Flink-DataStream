package org.linyureal.realtime.validation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.*;
import org.linyureal.realtime.model.RowChange;
import java.util.*;
import static org.linyureal.realtime.common.Jsons.*;

public final class RowValidator {
    private RowValidator() {}
    public static void validate(RowChange c) {
        if (!Contracts.TRADE_TABLES.contains(c.table) || !List.of("c", "u", "r").contains(c.operation))
            throw new IllegalArgumentException("Unsupported table/op; deletes require audited correction");
        ObjectNode r = object(c.rowJson);
        text(r, "id"); require(integer(r, "version") > 0, "Invalid version"); time(r, "updated_at");
        require(text(r, "order_id").equals(c.orderId), "Order routing mismatch");
        switch (c.table) {
            case "order_header":
                require(text(r, "id").equals(c.orderId), "Order ID mismatch");
                for (String f : List.of("user_id", "shop_id", "province_id", "city_id", "district_id")) text(r, f);
                require("CNY".equals(text(r, "currency")), "Only CNY supported");
                status(r, "CREATED", "PAID", "CANCELLED", "COMPLETED"); time(r, "created_at");
                if ("CANCELLED".equals(text(r, "status"))) time(r, "cancelled_at");
                positive(r, "line_count", "goods_cent", "payable_cent");
                require(integer(r, "freight_cent") >= 0, "Negative freight");
                require(Math.addExact(integer(r, "goods_cent"), integer(r, "freight_cent")) == integer(r, "payable_cent"), "Order total mismatch");
                break;
            case "order_line":
                for (String f : List.of("sku_id", "product_id", "category_id", "brand_id", "shop_id")) text(r, f);
                positive(r, "quantity", "unit_price_cent", "paid_cent");
                require(integer(r, "discount_cent") >= 0, "Negative discount");
                require(Math.subtractExact(Math.multiplyExact(integer(r, "unit_price_cent"), integer(r, "quantity")),
                        integer(r, "discount_cent")) == integer(r, "paid_cent"), "Line price/discount mismatch");
                break;
            case "payment_attempt":
                status(r, "PENDING", "SUCCEEDED", "FAILED"); positive(r, "amount_cent"); text(r, "channel");
                break;
            case "payment_ledger":
                status(r, "SUCCEEDED"); positive(r, "amount_cent"); time(r, "paid_at");
                text(r, "attempt_id"); text(r, "channel_transaction_id"); break;
            case "payment_line":
                text(r, "payment_id"); text(r, "order_line_id"); positive(r, "amount_cent"); break;
            case "refund_header":
                status(r, "APPLIED", "PROCESSING", "SUCCEEDED", "FAILED", "CLOSED");
                text(r, "payment_id"); positive(r, "amount_cent", "line_count");
                if ("SUCCEEDED".equals(text(r, "status"))) time(r, "succeeded_at"); break;
            case "refund_line":
                text(r, "refund_id"); text(r, "order_line_id"); positive(r, "amount_cent"); break;
            default: throw new IllegalArgumentException("Unrecognized table");
        }
    }
    private static void positive(ObjectNode row, String... fields) {
        for (String field : fields) require(integer(row, field) > 0, field + " must be positive");
    }
    private static void status(ObjectNode r, String... allowed) {
        require(Arrays.asList(allowed).contains(text(r, "status")), "Invalid status");
    }
    public static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
