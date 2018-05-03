package de.lindener.streaming.approximate.queries.models;

public class FrequentItemResultItem<T> {
    private T item;
    private long estimate;
    private long lowerBound;
    private long upperBound;

    public FrequentItemResultItem(T item, long estimate, long lowerBound, long upperBound) {
        this.item = item;
        this.estimate = estimate;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public long getEstimate() {
        return estimate;
    }

    public void setEstimate(long estimate) {
        this.estimate = estimate;
    }

    public long getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(long lowerBound) {
        this.lowerBound = lowerBound;
    }

    public long getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(long upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public String toString() {
        return "FrequentItemResultItem{" +
                "item=" + item +
                ", estimate=" + estimate +
                ", lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                '}';
    }

}
