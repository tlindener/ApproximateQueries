package de.lindener.streaming.queries.processing;

import de.lindener.streaming.queries.functions.HllSketchFunction;
import de.lindener.streaming.queries.models.HllSketchAggregation;
import de.lindener.streaming.queries.models.ThetaSketchAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class CountDistinctQueries {

    public static DataStream<HllSketchAggregation> runContinuousHll(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }

    public static DataStream<ThetaSketchAggregation> runContinuousTheta(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }


}
