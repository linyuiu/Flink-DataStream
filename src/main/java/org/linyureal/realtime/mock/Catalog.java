package org.linyureal.realtime.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.linyureal.realtime.common.Jsons;
import java.util.*;

/** Stable, relationally consistent fixture. Dimensions are metadata, not random independent IDs. */
public final class Catalog {
    private Catalog() {}
    public static Map<String, List<ObjectNode>> rows() {
        Map<String, List<ObjectNode>> data = new LinkedHashMap<>();
        add(data, "dim_category", base("C1", "家居用品", "0"));
        add(data, "dim_category", base("C2", "服饰", "0"));
        add(data, "dim_brand", base("B1", "示例家居", "0")); add(data, "dim_brand", base("B2", "示例服饰", "0"));
        add(data, "dim_shop", base("S1", "杭州旗舰店", "0")); add(data, "dim_shop", base("S2", "上海直营店", "0"));
        add(data, "dim_product", base("P1", "保温杯", "C1").put("category_id", "C1").put("brand_id", "B1"));
        add(data, "dim_product", base("P2", "纯棉T恤", "C2").put("category_id", "C2").put("brand_id", "B2"));
        add(data, "dim_sku", base("SKU1", "500ml蓝色", "P1").put("product_id", "P1").put("unit_price_cent", 8900));
        add(data, "dim_sku", base("SKU2", "白色M码", "P2").put("product_id", "P2").put("unit_price_cent", 12900));
        region(data, "310000", "上海市", "0", "PROVINCE"); region(data, "310100", "上海市", "310000", "CITY");
        region(data, "310115", "浦东新区", "310100", "DISTRICT"); region(data, "330000", "浙江省", "0", "PROVINCE");
        region(data, "330100", "杭州市", "330000", "CITY"); region(data, "330106", "西湖区", "330100", "DISTRICT");
        return data;
    }
    private static ObjectNode base(String id, String name, String parent) {
        ObjectNode n = Jsons.MAPPER.createObjectNode(); n.put("id", id); n.put("name", name); n.put("parent_id", parent);
        n.put("version", 1); n.put("updated_at", "2026-01-01 00:00:00"); return n;
    }
    private static void add(Map<String, List<ObjectNode>> data, String table, ObjectNode row) {
        data.computeIfAbsent(table, k -> new ArrayList<>()).add(row);
    }
    private static void region(Map<String, List<ObjectNode>> data, String id, String name, String parent, String level) {
        add(data, "dim_region", base(id, name, parent).put("region_level", level));
    }
}
