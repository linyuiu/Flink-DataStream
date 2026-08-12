package org.linyu.util;

import org.apache.flink.util.OutputTag;

public class DirtyOutputTags {
    public DirtyOutputTags() {
    }

    public static final OutputTag<String> JSON_DIRTY_TAG =
            new OutputTag<String>("json_dirty_data"){
            };

    public static final OutputTag<String> BUSINESS_DIRTY_TAG =
            new OutputTag<String>("business_dirty_data") {
            };
}
