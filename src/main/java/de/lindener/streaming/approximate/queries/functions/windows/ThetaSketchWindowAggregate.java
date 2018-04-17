package de.lindener.streaming.approximate.queries.functions.windows;

import de.lindener.streaming.approximate.queries.models.ThetaSketchAggregation;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class ThetaSketchWindowAggregate<T> implements AggregateFunction<T, ThetaSketchAggregation, ThetaSketchAggregation> {

    KeySelector keySelector;
    KeySelector valueSelector;

    public ThetaSketchWindowAggregate(KeySelector keySelector, KeySelector valueSelector) {
        this.keySelector = keySelector;
        this.valueSelector = valueSelector;
    }

    @Override
    public ThetaSketchAggregation createAccumulator() {
        return new ThetaSketchAggregation();
    }

    @Override
    public ThetaSketchAggregation add(T input, ThetaSketchAggregation aggregation) {
        Object key = null;
        Object value = null;
        try {
            key = keySelector.getKey(input);
            value = valueSelector.getKey(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
        aggregation.update(key, value);
        return aggregation;
    }


    @Override
    public ThetaSketchAggregation getResult(ThetaSketchAggregation aggregation) {
        return aggregation;
    }

    @Override
    public ThetaSketchAggregation merge(ThetaSketchAggregation aggregation, ThetaSketchAggregation acc) {
        return acc.merge(aggregation);
    }
}