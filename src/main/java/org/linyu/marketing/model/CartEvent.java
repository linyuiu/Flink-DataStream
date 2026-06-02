package org.linyu.marketing.model;

import java.io.Serializable;

public class CartEvent implements Serializable {
    public String eventId;
    public long userId;
    public long skuId;
    public long spuId;
    public int quantity;
    public long eventTime;
}