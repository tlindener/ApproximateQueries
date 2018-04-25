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
