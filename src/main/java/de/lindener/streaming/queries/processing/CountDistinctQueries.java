package de.lindener.streaming.queries.processing;

import de.lindener.streaming.queries.functions.HllSketchFunction;
import de.lindener.streaming.queries.functions.windows.HllSketchWindowAggregate;
import de.lindener.streaming.queries.functions.windows.ThetaSketchWindowAggregate;
import de.lindener.streaming.queries.models.HllSketchAggregation;
import de.lindener.streaming.queries.models.ThetaSketchAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class CountDistinctQueries {

    public static DataStream<HllSketchAggregation> runContinuousHll(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }

    public static <T> DataStream<HllSketchAggregation> runContinuousWindowHll(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
       HllSketchWindowAggregate<T> aggregateFunction = new HllSketchWindowAggregate(keySelector, valueSelector);
       SingleOutputStreamOperator stream = input.keyBy(keySelector).window(window).aggregate(aggregateFunction);

        return stream;
    }

    public static <T> DataStream<HllSketchAggregation> runContinuousWindowTheta(DataStream<T> input, KeySelector keySelector, KeySelector valueSelector, WindowAssigner window) {
        ThetaSketchWindowAggregate<T> aggregateFunction = new ThetaSketchWindowAggregate(keySelector, valueSelector);
        SingleOutputStreamOperator stream = input.keyBy(keySelector).window(window).aggregate(aggregateFunction);
        stream.print();
        return stream;
    }

    public static DataStream<ThetaSketchAggregation> runContinuousTheta(DataStream<?> input, KeySelector keySelector, KeySelector valueSelector, int emitMin) {
        HllSketchFunction hllSketchFunction = new HllSketchFunction(keySelector, valueSelector, emitMin);
        return input.keyBy(keySelector).flatMap(hllSketchFunction);
    }
}
