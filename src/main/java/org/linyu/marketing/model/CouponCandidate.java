package org.linyu.marketing.model;

import java.io.Serializable;

public class CouponCandidate implements Serializable {
    public long userId;
    public long skuId;
    public String ruleCode;
    public long eventTime;
    public long triggerTime;
    public String reason;
    public String sourceEventId;
}