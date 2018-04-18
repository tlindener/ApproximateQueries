package de.lindener.streaming.queries.functions.windows;

import com.yahoo.sketches.frequencies.ErrorType;
import de.lindener.streaming.queries.models.TopNAggregation;
import de.lindener.streaming.queries.models.TopNQueryResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class TopNWindowAggregate<T> implements AggregateFunction<T, TopNAggregation, TopNQueryResult> {
    private KeySelector valueSelector;
    private ErrorType type;
    private int numResults;

    public TopNWindowAggregate(KeySelector valueSelector, int numResults, ErrorType type) {
        this.valueSelector = valueSelector;
        this.numResults = numResults;
        this.type = type;
    }

    @Override
    public TopNAggregation createAccumulator() {
        return new TopNAggregation();
    }

    @Override
    public TopNAggregation add(T t, TopNAggregation topNAggregation) {
        topNAggregation.update(t);
        return topNAggregation;
    }

    @Override
    public TopNQueryResult<T> getResult(TopNAggregation topNAggregation) {
        return topNAggregation.getResult(type, numResults);
    }

    @Override
    public TopNAggregation merge(TopNAggregation topNAggregation, TopNAggregation acc1) {
        return topNAggregation.merge(acc1);
    }
}
