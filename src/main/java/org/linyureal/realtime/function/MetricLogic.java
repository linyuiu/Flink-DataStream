package org.linyureal.realtime.function;

import org.linyureal.realtime.common.Jsons;
import org.linyureal.realtime.model.*;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Predicate;
import static org.linyureal.realtime.validation.RowValidator.require;

public final class MetricLogic {
    private MetricLogic() {}
    public static TradeFact parse(String json) {
        try {
            TradeFact f = Jsons.MAPPER.readValue(json, TradeFact.class);
            require(f != null && List.of("CREATED", "CANCELLED", "PAID", "REFUNDED").contains(f.fact_type), "Unknown fact type");
            require(f.amount_cent > 0, "Invalid amount");
            for (String s : new String[]{f.fact_id, f.order_id, f.order_line_id, f.business_id, f.user_id,
                    f.product_id, f.sku_id, f.category_id, f.brand_id, f.shop_id, f.province_id, f.city_id, f.district_id})
                require(s != null && !s.isBlank() && !s.contains("|"), "Invalid fact ID/dimension");
            require("CNY".equals(f.currency), "Unsupported currency");
            LocalDate event = LocalDate.parse(f.event_date);
            if (List.of("PAID", "REFUNDED").contains(f.fact_type)) {
                LocalDate paid = LocalDate.parse(f.pay_date);
                require(!event.isBefore(paid), "Event before payment");
                if (f.fact_type.equals("PAID")) require(event.equals(paid), "Payment dates disagree");
            }
            require(f.fact_type.equals("REFUNDED") ? f.quantity == 0 : f.quantity > 0, "Invalid quantity");
            return f;
        } catch (java.io.IOException e) { throw new IllegalArgumentException("Invalid fact JSON", e); }
    }
    public static List<MetricInput> expand(TradeFact f) {
        String json = Jsons.write(f);
        String[][] dimensions = {{"ALL", "ALL"}, {"PRODUCT", f.product_id}, {"SKU", f.sku_id},
                {"CATEGORY", f.category_id}, {"BRAND", f.brand_id}, {"SHOP", f.shop_id},
                {"PROVINCE", f.province_id}, {"CITY", f.city_id}, {"DISTRICT", f.district_id}};
        List<MetricInput> result = new ArrayList<>();
        for (String[] d : dimensions) {
            result.add(new MetricInput(f.event_date, d[0], d[1], "EVENT", json));
            if (f.fact_type.equals("REFUNDED"))
                result.add(new MetricInput(f.pay_date, d[0], d[1], "NET_CORRECTION", json));
        }
        return result;
    }
    /** firstSeen is scoped to date+dimension; counts orders/users, never item rows. */
    public static long[] add(long[] previous, MetricInput input, Predicate<String> firstSeen) {
        TradeFact f = parse(input.factJson); long[] t = previous.clone();
        if (input.phase.equals("NET_CORRECTION")) {
            t[10] = Math.subtractExact(t[10], f.amount_cent); return t;
        }
        switch (f.fact_type) {
            case "CREATED":
                if (firstSeen.test("created:" + f.order_id)) t[0] = Math.addExact(t[0], 1);
                t[7] = Math.addExact(t[7], f.amount_cent); break;
            case "CANCELLED":
                if (firstSeen.test("cancelled:" + f.order_id)) t[1] = Math.addExact(t[1], 1); break;
            case "PAID":
                if (firstSeen.test("paid:" + f.order_id)) t[2] = Math.addExact(t[2], 1);
                if (firstSeen.test("user:" + f.user_id)) t[3] = Math.addExact(t[3], 1);
                t[6] = Math.addExact(t[6], f.quantity); t[8] = Math.addExact(t[8], f.amount_cent);
                t[10] = Math.addExact(t[10], f.amount_cent); break;
            case "REFUNDED":
                if (firstSeen.test("refundOrder:" + f.order_id)) t[4] = Math.addExact(t[4], 1);
                if (firstSeen.test("refundRequest:" + f.business_id)) t[5] = Math.addExact(t[5], 1);
                t[9] = Math.addExact(t[9], f.amount_cent); break;
            default: throw new IllegalArgumentException("Unknown fact");
        }
        return t;
    }
}
