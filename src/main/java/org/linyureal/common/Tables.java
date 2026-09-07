package org.linyureal.common;

import java.util.List;

/** Shared contracts without a dependency on Flink runtime classes. */
public final class Tables {
    private Tables() {}
    public static final List<String> DIMENSIONS=List.of(
            "dim_product","dim_sku","dim_category","dim_shop","dim_brand","dim_region");
}
