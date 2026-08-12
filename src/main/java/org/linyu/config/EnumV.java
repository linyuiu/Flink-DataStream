package org.linyu.config;

import org.apache.flink.util.OutputTag;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public  class EnumV {



    public static final ZoneId BUSINESS_ZONE =
            ZoneId.of("Asia/Shanghai");

    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );

    /**
     * 是否从 GMV 中扣除退款。

     * true：
     * 净GMV = pay_amount - refund_amount

     * false：
     * 毛GMV = pay_amount
     */
    public static final boolean DEDUCT_REFUNDS = true;
    public static final long OUTPUT_INTERVAL_MS =
            5_000L;

    public static final OutputTag<String> BUSINESS_DIRTY_TAG =
            new OutputTag<String>("business-dirty-data") {
            };
}
