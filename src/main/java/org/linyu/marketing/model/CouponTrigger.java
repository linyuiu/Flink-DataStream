package org.linyu.marketing.model;

import java.io.Serializable;

public class CouponTrigger implements Serializable {
    public String triggerId;
    public long userId;
    public long skuId;
    public String ruleCode;
    public long triggerTime;
    public String reason;
}