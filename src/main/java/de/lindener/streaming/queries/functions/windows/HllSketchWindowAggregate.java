package de.lindener.streaming.queries.functions.windows;

import de.lindener.streaming.queries.models.HllSketchAggregation;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class HllSketchWindowAggregate implements AggregateFunction<Object, HllSketchAggregation, HllSketchAggregation> {

    KeySelector keySelector;
    KeySelector valueSelector;

    public HllSketchWindowAggregate(KeySelector keySelector, KeySelector valueSelector) {
        this.keySelector = keySelector;
        this.valueSelector = valueSelector;
    }

    @Override
    public HllSketchAggregation createAccumulator() {
        return new HllSketchAggregation();
    }

    @Override
    public HllSketchAggregation add(Object input, HllSketchAggregation hllSketchAggregation) {
        Object key = null;
        Object value = null;
        try {
            key = keySelector.getKey(input);
            value = valueSelector.getKey(input);
        } catch (Exception e) {
            e.printStackTrace();
        }
        hllSketchAggregation.update(key, value);
        return hllSketchAggregation;
    }


    @Override
    public HllSketchAggregation getResult(HllSketchAggregation hllSketchAggregation) {
        return hllSketchAggregation;
    }

    @Override
    public HllSketchAggregation merge(HllSketchAggregation hllSketchAggregation, HllSketchAggregation acc1) {
        return acc1.mergeValues(hllSketchAggregation);
    }
}