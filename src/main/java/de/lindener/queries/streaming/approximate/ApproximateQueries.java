package de.lindener.queries.streaming.approximate;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import de.lindener.queries.streaming.approximate.aggregate.HllSketchAggregateFunction;
import de.lindener.queries.streaming.approximate.aggregate.HllSketchFold;
import de.lindener.queries.streaming.approximate.aggregate.SketchFold;
import de.lindener.queries.streaming.approximate.aggregate.WindowStreamTreeAggregation;
import de.lindener.queries.streaming.approximate.functions.HllSketchFunction;
import de.lindener.queries.streaming.approximate.functions.QuantileFunction;
import de.lindener.queries.streaming.approximate.functions.TopNQueryFunction;
import de.lindener.queries.streaming.approximate.models.HllAggregate;
import de.lindener.queries.streaming.approximate.models.TopNQueryResult;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

import java.util.HashMap;
import java.util.Map;

public class ApproximateQueries implements IApproximateQueries {
    public ApproximateQueries() {

    }

    @Override
    public DataStream<TopNQueryResult> continuousTopNResult(DataStream<?> inputStream, int n, int emitMin) {
        return inputStream.flatMap(new TopNQueryFunction());
    }

    @Override
    public DataStream<com.yahoo.sketches.quantiles.ItemsSketch<?>> continuousQuantiles(DataStream<?> inputStream, int n, int emitMin) {
        return inputStream.flatMap(new QuantileFunction());

    }

    @Override
    public DataStream<Tuple2<Object, HllSketch>> continuousHllDistinctCount(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin, TgtHllType tgtHllType, int lgk) {
        return inputStream.keyBy(fieldKey).flatMap(new HllSketchFunction(fieldId, emitMin, tgtHllType, lgk));
    }

    @Override
    public DataStream<Tuple2<Object, HllSketch>> continuousHllDistinctCount(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin) {
        return inputStream.keyBy(fieldKey).flatMap(new HllSketchFunction(fieldId, emitMin));
    }

    @Override
    public DataStream<HashMap<Object, HllSketch>> continuousHllDistinctCountAggregate(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int window) {
        if (window <= 0) {
            throw new IllegalArgumentException();
        }
        return continuousHllDistinctCountAggregate(inputStream, fieldKey, fieldId, window, TgtHllType.HLL_4, 4);
    }

    @Override
    public DataStream<HashMap<Object, HllSketch>> continuousHllDistinctCountAggregate(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int window, TgtHllType tgtHllType, int lgk) {
        if (window <= 0) {
            throw new IllegalArgumentException();
        }
        int emitMin = (int) Math.ceil(window / 2);
        DataStream<Tuple2<Object, HllSketch>> keyedStream = continuousHllDistinctCount(inputStream, fieldKey, fieldId, emitMin, tgtHllType, lgk);
        WindowStreamTreeAggregation<Object, HllSketch, HashMap<Object, HllSketch>, HashMap<Object, HllSketch>> aggregation = new WindowStreamTreeAggregation<Object, HllSketch, HashMap<Object, HllSketch>, HashMap<Object, HllSketch>>(new HllSketchFold(), new HllSketchAggregateFunction(), new HashMap<Object, HllSketch>(), window, false);

        return aggregation.run(keyedStream);
    }




}
