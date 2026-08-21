package org.linyu.map;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


import java.io.Serializable;
import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderDetailRealTime implements Serializable {
    @JsonProperty("order_id")
    public String orderId;

    @JsonProperty("user_id")
    public String userId;

    @JsonProperty("sku_id")
    public String skuId;

    @JsonProperty("pay_amount")
    public BigDecimal payAmount;

    @JsonProperty("refund_amount")
    public BigDecimal refundAmount;

    @JsonProperty("order_status")
    public String orderStatus;

    @JsonProperty("create_time")
    public String createTime;

    @JsonProperty("pay_time")
    public String payTime;

    @JsonProperty("dt")
    public String dt;
    @JsonProperty("order_version")
    public Long orderVersion;

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("record_grain")
    public String recordGrain;

    @JsonProperty("currency_code")
    public String currencyCode;

    /*
     * 推荐上游增加的事件版本时间。
     *
     * 示例：
     * "update_time": "2026-07-31 08:30:15"
     */
    @JsonProperty("update_time")
    public String updateTime;

    public OrderDetailRealTime() {
    }

}
