package org.commerce.mock;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.commerce.common.Json;

import java.util.*;

public final class Catalog {
    public static final int SKUS = 1000, SHOPS = 100, CATEGORIES = 20, BRANDS = 10;

    private Catalog() {}

    public static String product(int sku) {
        return "P" + ((sku + 1) / 2);
    }

    public static String category(int sku) {
        return "C" + (((sku + 1) / 2 - 1) % CATEGORIES + 1);
    }

    public static String brand(int sku) {
        return "B" + (((sku + 1) / 2 - 1) % BRANDS + 1);
    }

    public static Map<String, List<ObjectNode>> rows() {
        Map<String, List<ObjectNode>> result = new LinkedHashMap<>();
        for (int i = 1; i <= CATEGORIES; i++)
            add(result, "dim_category", row("C" + i, "品类" + i, "0", "{}"));
        for (int i = 1; i <= BRANDS; i++)
            add(result, "dim_brand", row("B" + i, "品牌" + i, "0", "{}"));
        for (int i = 1; i <= SHOPS; i++) add(result, "dim_shop", row("S" + i, "店铺" + i, "0", "{}"));
        for (int i = 1; i <= SKUS; i++) {
            if ((i & 1) == 1)
                add(
                        result,
                        "dim_product",
                        row(
                                product(i),
                                "商品" + product(i),
                                category(i),
                                Json.MAPPER
                                        .createObjectNode()
                                        .put("category_id", category(i))
                                        .put("brand_id", brand(i))
                                        .toString()));
            add(result, "dim_sku", row("SKU" + i, "规格" + i, product(i), "{}"));
        }
        add(result, "dim_region", row("330000", "浙江省", "0", "{}").put("region_level", "PROVINCE"));
        add(result, "dim_region", row("330100", "杭州市", "330000", "{}").put("region_level", "CITY"));
        add(
                result,
                "dim_region",
                row("330106", "西湖区", "330100", "{}").put("region_level", "DISTRICT"));
        add(result, "dim_region", row("310000", "上海市", "0", "{}").put("region_level", "PROVINCE"));
        add(result, "dim_region", row("310100", "上海市", "310000", "{}").put("region_level", "CITY"));
        add(
                result,
                "dim_region",
                row("310115", "浦东新区", "310100", "{}").put("region_level", "DISTRICT"));
        return result;
    }

    private static ObjectNode row(String id, String name, String parent, String attributes) {
        return Json.MAPPER
                .createObjectNode()
                .put("id", id)
                .put("name", name)
                .put("parent_id", parent)
                .put("version", 1)
                .put("attributes_json", attributes);
    }

    private static void add(Map<String, List<ObjectNode>> result, String table, ObjectNode row) {
        result.computeIfAbsent(table, k -> new ArrayList<>()).add(row);
    }
}
