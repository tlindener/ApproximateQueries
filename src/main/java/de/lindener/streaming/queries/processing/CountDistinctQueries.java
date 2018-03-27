package de.lindener.streaming.queries.processing;

import de.lindener.streaming.queries.functions.HllSketchFunction;
import de.lindener.streaming.queries.functions.windows.HllSketchWindowAggregate;
import de.lindener.streaming.queries.functions.windows.ThetaSketchWindowAggregate;
import de.lindener.streaming.queries.models.HllSketchAggregation;
import de.lindener.streaming.queries.models.ThetaSketchAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

public class CountDistinctQueries {

    public static DataStream<HllSketchAggregation> runContinuousHll(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }

    public static DataStream<HllSketchAggregation> runContinuousWindowHll(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
        HllSketchWindowAggregate aggregateFunction = new HllSketchWindowAggregate(keySelector, valueSelector);
        return input.keyBy(keySelector).window(window).aggregate(aggregateFunction);
    }

    public static DataStream<HllSketchAggregation> runContinuousWindowTheta(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
        ThetaSketchWindowAggregate aggregateFunction = new ThetaSketchWindowAggregate(keySelector, valueSelector);
        return input.keyBy(keySelector).window(window).aggregate(aggregateFunction);
    }

    public static DataStream<ThetaSketchAggregation> runContinuousTheta(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }


}
