package org.linyu.map;


import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单上一次对GMV的贡献状态。
 */
public class OrderContributionState
        implements Serializable {
    public String bizDate;
    public BigDecimal contribution;
    public Long lastVersion;

    public OrderContributionState() {
    }

    public OrderContributionState(String bizDate, BigDecimal contribution, Long lastVersion) {
        this.bizDate = bizDate;
        this.contribution = contribution;
        this.lastVersion = lastVersion;
    }
}
