package org.linyureal.validation;

import com.fasterxml.jackson.databind.JsonNode;
import org.linyureal.common.Json;
import org.linyureal.model.cdc.Change;
import java.time.LocalDateTime;
import java.time.format.*;
import java.util.*;

public final class TradeValidator {
    public static final List<String> TABLES = Collections.unmodifiableList(Arrays.asList(
            "trade_order", "trade_order_item", "trade_payment", "trade_payment_item", "trade_refund", "trade_refund_item"));
    public static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
            .withResolverStyle(ResolverStyle.STRICT);
    private TradeValidator() {}
    public static String date(JsonNode row, String field) {
        return LocalDateTime.parse(Json.text(row, field), TIME).toLocalDate().toString();
    }
    public static void validate(Change c) {
        if (!TABLES.contains(c.table)) throw new IllegalArgumentException("Unsupported table: " + c.table);
        if (!Arrays.asList("c", "u", "r").contains(c.op)) throw new IllegalArgumentException("Deletes/unknown operations require reconciliation: " + c.op);
        Json.text(c.after, "id"); Json.text(c.after, "order_id");
        if (Json.number(c.after, "version") <= 0) throw new IllegalArgumentException("version must be positive");
        date(c.after, "update_time");
        switch (c.table) {
            case "trade_order":
                require(c.after, "shop_id", "province_code", "city_code", "district_code");
                if (!"CNY".equals(Json.text(c.after, "currency_code"))) throw new IllegalArgumentException("Only CNY supported");
                if (!Arrays.asList("CREATED", "PAID", "CANCELLED", "COMPLETED").contains(Json.text(c.after,"status")))
                    throw new IllegalArgumentException("Unknown order status");
                positive(c.after,"item_count"); positive(c.after,"goods_cent"); nonnegative(c.after,"freight_cent");
                if (Json.number(c.after,"payable_cent") != Math.addExact(Json.number(c.after,"goods_cent"),Json.number(c.after,"freight_cent")))
                    throw new IllegalArgumentException("Order total mismatch");
                break;
            case "trade_order_item":
                require(c.after,"sku_id","product_id","category_id","brand_id","shop_id");
                positive(c.after,"quantity"); positive(c.after,"payable_cent"); break;
            case "trade_payment":
                if (!"SUCCEEDED".equals(Json.text(c.after,"status"))) throw new IllegalArgumentException("Payment table is SUCCESS ledger only");
                require(c.after,"channel_transaction_id"); positive(c.after,"amount_cent"); date(c.after,"pay_time"); break;
            case "trade_payment_item":
                require(c.after,"payment_id","order_item_id"); positive(c.after,"amount_cent"); break;
            case "trade_refund":
                require(c.after,"payment_id");
                if (!Arrays.asList("APPLIED","PROCESSING","SUCCEEDED","FAILED","CLOSED").contains(Json.text(c.after,"status")))
                    throw new IllegalArgumentException("Unknown refund status");
                positive(c.after,"item_count"); positive(c.after,"amount_cent");
                if ("SUCCEEDED".equals(Json.text(c.after,"status"))) date(c.after,"refund_time");
                break;
            case "trade_refund_item":
                require(c.after,"refund_id","order_item_id"); positive(c.after,"amount_cent"); break;
            default: throw new IllegalArgumentException("Unsupported table");
        }
    }
    private static void require(JsonNode row, String... fields) { for(String f:fields) Json.text(row,f); }
    private static void positive(JsonNode row,String field) {
        if(Json.number(row,field)<=0) throw new IllegalArgumentException(field+" must be positive");
    }
    private static void nonnegative(JsonNode row,String field) {
        if(Json.number(row,field)<0) throw new IllegalArgumentException(field+" must be nonnegative");
    }
}
