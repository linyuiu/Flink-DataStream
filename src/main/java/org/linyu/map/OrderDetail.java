package org.linyu.map;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

public class OrderDetail {
    private String orderId;
    private String userId;
    private String skuId;
    private String payAmount;
    private String refundAmount;
    private String orderStatus;
    private LocalDateTime CreateTime ;
    private LocalDateTime payTime ;
    private LocalDate dt;

    public String getOrderId() {
        return orderId;
    }

    public String getUserId() {
        return userId;
    }

    public String getSkuId() {
        return skuId;
    }

    public String getPayAmount() {
        return payAmount;
    }

    public String getRefundAmount() {
        return refundAmount;
    }

    public String getOrderStatus() {
        return orderStatus;
    }

    public LocalDateTime getCreateTime() {
        return CreateTime;
    }

    public LocalDateTime getPayTime() {
        return payTime;
    }

    public LocalDate getDt() {
        return dt;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public void setPayAmount(String payAmount) {
        this.payAmount = payAmount;
    }

    public void setRefundAmount(String refundAmount) {
        this.refundAmount = refundAmount;
    }

    public void setOrderStatus(String orderStatus) {
        this.orderStatus = orderStatus;
    }

    public void setCreateTime(LocalDateTime createTime) {
        CreateTime = createTime;
    }

    public void setPayTime(LocalDateTime payTime) {
        this.payTime = payTime;
    }

    public void setDt(LocalDate dt) {
        this.dt = dt;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", skuId='" + skuId + '\'' +
                ", payAmount='" + payAmount + '\'' +
                ", refundAmount='" + refundAmount + '\'' +
                ", orderStatus='" + orderStatus + '\'' +
                ", CreateTime=" + CreateTime +
                ", payTime=" + payTime +
                ", dt=" + dt +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderDetail that = (OrderDetail) o;
        return Objects.equals(orderId, that.orderId) && Objects.equals(userId, that.userId) && Objects.equals(skuId, that.skuId) && Objects.equals(payAmount, that.payAmount) && Objects.equals(refundAmount, that.refundAmount) && Objects.equals(orderStatus, that.orderStatus) && Objects.equals(CreateTime, that.CreateTime) && Objects.equals(payTime, that.payTime) && Objects.equals(dt, that.dt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, userId, skuId, payAmount, refundAmount, orderStatus, CreateTime, payTime, dt);
    }
}
