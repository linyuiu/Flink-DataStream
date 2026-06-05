package org.linyu.map;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 某个订单当前应该贡献给今日 GMV 的金额。
 */
public class OrderGmvContribution implements Serializable {
    public String orderId;
    public String dt;
    public BigDecimal gmv;
    public long paidOrderCount;

    public OrderGmvContribution() {
    }

    public OrderGmvContribution(
            String orderId,
            String dt,
            BigDecimal gmv,
            long paidOrderCount
    ) {
        this.orderId = orderId;
        this.dt = dt;
        this.gmv = gmv;
        this.paidOrderCount = paidOrderCount;
    }

    public static OrderGmvContribution empty(String orderId) {
        return new OrderGmvContribution(orderId, "", BigDecimal.ZERO, 0L);
    }

    public boolean isEmpty() {
        return !OrderEvent.hasText(dt);
    }
}
