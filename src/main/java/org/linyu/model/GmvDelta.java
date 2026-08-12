package org.linyu.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class GmvDelta implements Serializable {
    private String bizDate;
    private BigDecimal amount;
    private String orderId;

    public GmvDelta() {
    }

    public GmvDelta(String bizDate, BigDecimal amount, String orderId) {
        this.bizDate = bizDate;
        this.amount = amount;
        this.orderId = orderId;
    }

    public String getBizDate() {
        return bizDate;
    }

    public void setBizDate(String bizDate) {
        this.bizDate = bizDate;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
}
