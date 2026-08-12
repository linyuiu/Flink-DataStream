package org.linyu.config;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class BusinessTime {
    public BusinessTime() {
    }

    public static final ZoneId ZONE_ID =
            ZoneId.of("Asia/Shanghai");

    public static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern(
                    "yyyy-MM-dd HH:mm:ss"
            );

    public static LocalDateTime parseDateTime(
            String value
    ) {
        return LocalDateTime.parse(
                value,
                DATE_TIME_FORMATTER
        );
    }

    public static String extracDate(
            String dateTime
    ) {
        if (dateTime == null || dateTime.trim().isEmpty()) {
            return null;
        }
        return parseDateTime(dateTime).toLocalDate().toString();
    }

    public static String formatNow() {
        return LocalDateTime
                .now(ZONE_ID)
                .format(DATE_TIME_FORMATTER);
    }
}
