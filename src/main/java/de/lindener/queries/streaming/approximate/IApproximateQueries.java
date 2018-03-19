package de.lindener.queries.streaming.approximate;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import de.lindener.queries.streaming.approximate.models.TopNQueryResult;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;


public interface IApproximateQueries {
    DataStream<TopNQueryResult> continuousTopNResult(DataStream<?> inputStream, int n, int emitMin);

    DataStream<com.yahoo.sketches.quantiles.ItemsSketch<?>> continuousQuantiles(DataStream<?> inputStream, int n, int emitMin);

    DataStream<Tuple2<Object, HllSketch>> continuousHllDistinctCount(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin, TgtHllType tgtHllType, int lgk);

    DataStream<Tuple2<Object, HllSketch>> continuousHllDistinctCount(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin);

    DataStream<HashMap<Object, HllSketch>> continuousHllDistinctCountAggregate(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin);

    DataStream<HashMap<Object, HllSketch>> continuousHllDistinctCountAggregate(DataStream<? extends Tuple> inputStream, int fieldKey, int fieldId, int emitMin, TgtHllType tgtHllType, int lgk);
}
