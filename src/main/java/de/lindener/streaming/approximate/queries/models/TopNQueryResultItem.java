package de.lindener.streaming.approximate.queries.models;

public class TopNQueryResultItem<T> {
    private T item;
    private long estimate;
    private long lowerBound;
    private long upperBound;

    public TopNQueryResultItem(T item, long estimate, long lowerBound, long upperBound) {
        this.item = item;
        this.estimate = estimate;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public String toString() {
        return "TopNQueryResultItem{" +
                "item=" + item +
                ", estimate=" + estimate +
                ", lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                '}';
    }
}
