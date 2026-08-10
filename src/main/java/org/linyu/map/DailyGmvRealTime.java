package org.linyu.map;

import java.io.Serializable;
import java.math.BigDecimal;

public class DailyGmvRealTime implements Serializable {


    public String bizDate;
    public BigDecimal gmv;
    public String updateTime;

    public DailyGmvRealTime() {
    }

    public DailyGmvRealTime(
            String bizDate,
            BigDecimal gmv,
            String updateTime) {

        this.bizDate = bizDate;
        this.gmv = gmv;
        this.updateTime = updateTime;
    }
}

