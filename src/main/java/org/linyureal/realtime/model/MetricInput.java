package org.linyureal.realtime.model;

public class MetricInput {
    public String bizDate, dimensionType, dimensionId, phase, factJson;
    public MetricInput() {}
    public MetricInput(String date, String type, String id, String phase, String json) {
        this.bizDate = date; this.dimensionType = type; this.dimensionId = id; this.phase = phase; this.factJson = json;
    }
    public String key() { return bizDate + "|" + dimensionType + "|" + dimensionId; }
}
