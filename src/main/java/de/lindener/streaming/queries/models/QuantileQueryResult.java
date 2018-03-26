package de.lindener.streaming.queries.models;

import com.yahoo.sketches.quantiles.ItemsSketch;

public class QuantileQueryResult<T> {
    ItemsSketch<T> sketch;

    public QuantileQueryResult() {

    }

    public QuantileQueryResult(ItemsSketch<T> sketch) {
        this.sketch = sketch;
    }

    public ItemsSketch getSketch() {
        return sketch;
    }

    public void setSketch(ItemsSketch sketch) {
        this.sketch = sketch;
    }


}
