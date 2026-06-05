package org.linyu.map;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 一条订单对当天 GMV 产生的增量。
 */
public class GmvDelta implements Serializable {
    public String orderId;
    public String dt;
    public BigDecimal gmvDelta;
    public long paidOrderCountDelta;

    public GmvDelta() {
    }

    public GmvDelta(
            String orderId,
            String dt,
            BigDecimal gmvDelta,
            long paidOrderCountDelta
    ) {
        this.orderId = orderId;
        this.dt = dt;
        this.gmvDelta = gmvDelta;
        this.paidOrderCountDelta = paidOrderCountDelta;
    }
}
