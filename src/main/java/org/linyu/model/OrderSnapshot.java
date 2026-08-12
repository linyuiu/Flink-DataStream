package org.linyu.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)

public class OrderSnapshot implements Serializable {
    private String orderId;
    private BigDecimal payAmount;
    private BigDecimal refundAmount;
    private String orderStatus;
    private String payTime;
    private Long eventVersion;



    @JsonProperty("Order_id")
    public String getOrderId() {
        return orderId;
    }
    @JsonProperty("Order_id")
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    @JsonProperty("pay_amount")
    public BigDecimal getPayAmount() {
        return payAmount;
    }

    @JsonProperty("pay_amount")
    public void setPayAmount(BigDecimal payAmount) {
        this.payAmount = payAmount;
    }

    @JsonProperty("refund_amount")
    public BigDecimal getRefundAmount() {
        return refundAmount;
    }

    @JsonProperty("refund_amount")
    public void setRefundAmount(BigDecimal refundAmount) {
        this.refundAmount = refundAmount;
    }

    @JsonProperty("order_status")
    public String getOrderStatus() {
        return orderStatus;
    }

    @JsonProperty("order_status")
    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    @JsonProperty("pay_time")
    public String getPayTime() {
        return payTime;
    }

    @JsonProperty("pay_time")
    public void setPayTime(String payTime) {
        this.payTime = payTime;
    }

    @JsonProperty("EventVersion")
    public Long getEventVersion() {
        return eventVersion;
    }

    @JsonProperty("Order_id")
    public void setEventVersion(Long eventVersion) {
        this.eventVersion = eventVersion;
    }
}
