package de.lindener.streaming.approximate.queries;

import de.lindener.streaming.approximate.queries.models.ThetaSketchAggregation;
import de.lindener.streaming.approximate.queries.functions.HllSketchFunction;
import de.lindener.streaming.approximate.queries.functions.windows.HllSketchWindowAggregate;
import de.lindener.streaming.approximate.queries.functions.windows.ThetaSketchWindowAggregate;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

public class CountDistinctQueries {

    public static <T> DataStream<HllSketchAggregation> runContinuousHll(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }

    public static <T> DataStream<HllSketchAggregation> runContinuousWindowHll(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
        HllSketchWindowAggregate<T> aggregateFunction = new HllSketchWindowAggregate(keySelector, valueSelector);
        return input.keyBy(keySelector).window(window).aggregate(aggregateFunction);
    }

    public static <T> DataStream<HllSketchAggregation> runContinuousWindowTheta(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
        ThetaSketchWindowAggregate<T> aggregateFunction = new ThetaSketchWindowAggregate(keySelector, valueSelector);

        return input.keyBy(keySelector).window(window).aggregate(aggregateFunction);
    }

    public static <T> DataStream<ThetaSketchAggregation> runContinuousTheta(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }
}
