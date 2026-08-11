package org.linyu.config;

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

}
