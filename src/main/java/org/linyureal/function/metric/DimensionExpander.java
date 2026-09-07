package org.linyureal.function.metric;

import org.linyureal.model.metric.*;
import java.time.LocalDate;
import java.util.*;

public final class DimensionExpander {
    private DimensionExpander() {}
    public static List<MetricDelta> expand(ItemFact f) {
        if(f==null || f.fact_id==null || f.fact_id.isEmpty() || f.amount_cent<=0 || !"CNY".equals(f.currency_code))
            throw new IllegalArgumentException("Invalid money fact");
        if(!Arrays.asList("PAYMENT","REFUND").contains(f.fact_type)) throw new IllegalArgumentException("Unknown fact type");
        for(String id:new String[]{f.order_id,f.order_item_id,f.sku_id})
            if(id==null || id.trim().isEmpty()) throw new IllegalArgumentException("Missing fact identity");
        LocalDate.parse(f.pay_date); LocalDate.parse(f.event_date);
        if("PAYMENT".equals(f.fact_type) && !f.pay_date.equals(f.event_date))
            throw new IllegalArgumentException("Payment dates disagree");
        if(LocalDate.parse(f.event_date).isBefore(LocalDate.parse(f.pay_date)))
            throw new IllegalArgumentException("Refund precedes payment date");
        String[][] dimensions={{"PRODUCT",f.product_id},{"CATEGORY",f.category_id},{"SHOP",f.shop_id},
                {"BRAND",f.brand_id},{"PROVINCE",f.province_code},{"CITY",f.city_code},{"DISTRICT",f.district_code}};
        List<MetricDelta> output=new ArrayList<>();
        for(String[] d:dimensions) {
            if(d[1]==null || d[1].isEmpty() || d[1].contains("|")) throw new IllegalArgumentException("Missing/invalid dimension");
            if("PAYMENT".equals(f.fact_type))
                output.add(new MetricDelta(f.pay_date,d[0],d[1],f.currency_code,f.amount_cent,0,f.amount_cent));
            else {
                output.add(new MetricDelta(f.event_date,d[0],d[1],f.currency_code,0,f.amount_cent,0));
                output.add(new MetricDelta(f.pay_date,d[0],d[1],f.currency_code,0,0,-f.amount_cent));
            }
        }
        return output;
    }
}
