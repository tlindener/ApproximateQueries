package de.lindener.streaming.approximate.queries.models;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

public class FrequentItemAggregation<T> {

    private ItemsSketch<T> itemsSketch;

    public FrequentItemAggregation() {
        this(4096 * 8);
    }

    public FrequentItemAggregation(int mapSize) {
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

    public FrequentItemAggregation merge(FrequentItemAggregation edgeValue) {
        this.itemsSketch.merge(edgeValue.getItemsSketch());
        return this;
    }

    public FrequentItemResult<T> getResult(ErrorType type) {
        return FrequentItemResult.fromSketch(itemsSketch, type);
    }
}
