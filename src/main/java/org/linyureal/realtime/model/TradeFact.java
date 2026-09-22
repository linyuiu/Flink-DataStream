package org.linyureal.realtime.model;

/** Immutable item-grain fact. REFUNDED is monetary refund, not a return-of-goods fact. */
public class TradeFact {
    public String fact_id, fact_type, order_id, order_line_id, business_id, user_id;
    public String product_id, sku_id, category_id, brand_id, shop_id;
    public String province_id, city_id, district_id, currency, event_date, pay_date;
    public long amount_cent, quantity;
    public TradeFact() {}
}
