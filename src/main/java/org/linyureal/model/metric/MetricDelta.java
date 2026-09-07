package org.linyureal.model.metric;

import java.io.Serializable;

public class MetricDelta implements Serializable {
    public String bizDate, dimensionType, dimensionId, currency;
    public long paid, refunded, net;
    public MetricDelta() {}
    public MetricDelta(String date, String type, String id, String currency, long paid, long refunded, long net) {
        this.bizDate=date; this.dimensionType=type; this.dimensionId=id; this.currency=currency;
        this.paid=paid; this.refunded=refunded; this.net=net;
    }
    public String key() { return bizDate + "|" + dimensionType + "|" + dimensionId + "|" + currency; }
}
