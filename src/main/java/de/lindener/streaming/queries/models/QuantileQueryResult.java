package de.lindener.streaming.queries.models;

import com.yahoo.sketches.quantiles.ItemsSketch;

import java.util.Arrays;

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

    public T[] getQuantiles() {
        T[] quantiles = sketch.getQuantiles(new double[]{0, 0.5, 1});
        System.out.println("Min, Median, Max values");
        System.out.println(Arrays.toString(quantiles));
        return quantiles;
    }
}
