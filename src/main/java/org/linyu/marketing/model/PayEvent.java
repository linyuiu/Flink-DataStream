package org.linyu.marketing.model;

import java.io.Serializable;

public class PayEvent implements Serializable {
    public String eventId;
    public long orderId;
    public long userId;
    public long skuId;
    public long payTime;
}