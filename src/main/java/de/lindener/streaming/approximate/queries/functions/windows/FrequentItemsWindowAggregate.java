package de.lindener.streaming.approximate.queries.functions.windows;

import com.yahoo.sketches.frequencies.ErrorType;
import de.lindener.streaming.approximate.queries.models.FrequentItemAggregation;
import de.lindener.streaming.approximate.queries.models.FrequentItemResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class FrequentItemsWindowAggregate<T> implements AggregateFunction<T, FrequentItemAggregation, FrequentItemResult> {
    private KeySelector valueSelector;
    private ErrorType type;
    private int numResults;

    public FrequentItemsWindowAggregate(KeySelector valueSelector, int numResults, ErrorType type) {
        this.valueSelector = valueSelector;
        this.numResults = numResults;
        this.type = type;
    }

    @Override
    public FrequentItemAggregation createAccumulator() {
        return new FrequentItemAggregation();
    }

    @Override
    public FrequentItemAggregation add(T t, FrequentItemAggregation frequentItemAggregation) {
        frequentItemAggregation.update(t);
        return frequentItemAggregation;
    }

    @Override
    public FrequentItemResult<T> getResult(FrequentItemAggregation frequentItemAggregation) {
        return frequentItemAggregation.getResult(type, numResults);
    }

    @Override
    public FrequentItemAggregation merge(FrequentItemAggregation frequentItemAggregation, FrequentItemAggregation acc1) {
        return frequentItemAggregation.merge(acc1);
    }
}
