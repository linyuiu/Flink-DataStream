package org.commerce.function;

import org.commerce.common.Contract;

import java.time.*;

/** 以北京时间自然日计算绝对关闭时刻，避免状态已清理、旧事件却仍能重新计入。 */
public final class Horizons {
    private Horizons() {}

    public static String date(long millis) {
        return Instant.ofEpochMilli(millis).atZone(Contract.ZONE).toLocalDate().toString();
    }

    public static long expires(String businessDate, long days) {
        // 包含业务日自身及其后的 days 个日历日，因此在 days + 1 天的零点关闭。
        return LocalDate.parse(businessDate)
                .plusDays(days + 1)
                .atStartOfDay(Contract.ZONE)
                .toInstant()
                .toEpochMilli();
    }

    public static boolean open(String date, long now, long days) {
        return now < expires(date, days);
    }
}
