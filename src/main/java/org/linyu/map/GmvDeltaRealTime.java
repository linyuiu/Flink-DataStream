package org.linyu.map;

import java.math.BigDecimal;

public class GmvDeltaRealTime {
    public String bizDate;
    public BigDecimal deltaAmount;
    public String orderId;

    public GmvDeltaRealTime() {
    }

    public GmvDeltaRealTime(
            String bizDate,
            BigDecimal deltaAmount,
            String orderId) {

        this.bizDate = bizDate;
        this.deltaAmount = deltaAmount;
        this.orderId = orderId;
    }
}
