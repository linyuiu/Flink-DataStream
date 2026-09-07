package org.linyureal.model.metric;

import java.io.Serializable;

/** Immutable business ledger projection. Amounts are integer CNY cents. */
public class ItemFact implements Serializable {
    public String fact_id;
    public String fact_type;
    public String order_id;
    public String order_item_id;
    public String product_id;
    public String sku_id;
    public String category_id;
    public String brand_id;
    public String shop_id;
    public String province_code;
    public String city_code;
    public String district_code;
    public String currency_code;
    public String pay_date;
    public String event_date;
    public long amount_cent;
    public ItemFact() {}
}
