package org.linyu.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;

public class DailyGmvRow implements Serializable {

    @JsonProperty("biz_date")
    private String bizDate;

    @JsonProperty("gmv")
    private BigDecimal gmv;

    @JsonProperty("update_time")
    private String updateTime;

    public DailyGmvRow() {
    }

    public DailyGmvRow(String bizDate, BigDecimal gmv, String updateTime) {
        this.bizDate = bizDate;
        this.gmv = gmv;
        this.updateTime = updateTime;
    }

    public String getBizDate() {
        return bizDate;
    }

    public void setBizDate(String bizDate) {
        this.bizDate = bizDate;
    }

    public BigDecimal getGmv() {
        return gmv;
    }

    public void setGmv(BigDecimal gmv) {
        this.gmv = gmv;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
}
