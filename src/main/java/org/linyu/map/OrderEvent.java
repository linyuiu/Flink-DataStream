package org.linyu.map;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Kafka 订单明细消息对应的内部对象。
 */
public class OrderEvent implements Serializable {

    private static final ZoneId BUSINESS_ZONE = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public String orderId;
    public String userId;
    public BigDecimal payAmount = BigDecimal.ZERO;
    public BigDecimal refundAmount = BigDecimal.ZERO;
    public String orderStatus;
    public String payTime;
    public String dt;

    public boolean isPositiveGmvStatus() {
        return "PAID".equals(orderStatus) || "FINISHED".equals(orderStatus);
    }

    public boolean isTodayTrade() {
        return hasTradeTime() && currentBusinessDate().equals(tradeDate());
    }

    public boolean hasTradeTime() {
        return hasText(payTime);
    }

    public String tradeDate() {
        LocalDateTime payDateTime = LocalDateTime.parse(payTime, DATE_TIME_FORMATTER);
        return payDateTime.atZone(BUSINESS_ZONE).toLocalDate().toString();
    }

    public BigDecimal netGmv() {
        return payAmount
                .subtract(refundAmount)
                .max(BigDecimal.ZERO)
                .setScale(2, RoundingMode.HALF_UP);
    }

    private static String currentBusinessDate() {
        return LocalDate.now(BUSINESS_ZONE).toString();
    }

    public static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
