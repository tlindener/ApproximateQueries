package de.lindener.streaming.approximate.queries.models;

public class HllEstimate {

    private Object key;
    private Double estimate;

    public HllEstimate(Object key, Double estimate) {
        this.key = key;
        this.estimate = estimate;
    }

    public HllEstimate() {
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Double getEstimate() {
        return estimate;
    }

    public void setEstimate(Double estimate) {
        this.estimate = estimate;
    }
}
