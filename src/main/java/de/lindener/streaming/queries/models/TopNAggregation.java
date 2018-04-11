package de.lindener.streaming.queries.models;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

public class TopNAggregation<T> {

    private ItemsSketch<T> itemsSketch;

    public TopNAggregation() {
        this(64);
    }

    public TopNAggregation(int mapSize) {
        itemsSketch = new ItemsSketch<>(mapSize);
    }

    public ItemsSketch<T> getItemsSketch() {
        return itemsSketch;
    }

    public void setItemsSketch(ItemsSketch<T> itemsSketch) {
        this.itemsSketch = itemsSketch;
    }

    public void update(T item) {
        itemsSketch.update(item);
    }

    public TopNAggregation merge(TopNAggregation edgeValue) {
        this.itemsSketch.merge(edgeValue.getItemsSketch());
        return this;
    }

    public TopNQueryResult<T> getResult(ErrorType type, int n) {
        return TopNQueryResult.fromSketch(itemsSketch, type, n);
    }
}
