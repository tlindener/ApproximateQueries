package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.hll.TgtHllType;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ThetaSketchFunction<T> extends RichFlatMapFunction<T, Tuple2<Object, HllSketchAggregation>> {
    private KeySelector keySelector;
    private KeySelector aggregateSelector;
    private HllSketchAggregation aggregation;
    private TgtHllType tgtHllType;
    private int lgk;

    public ThetaSketchFunction(KeySelector keySelector, KeySelector aggregateSelector) {
        this(keySelector, aggregateSelector, 1, TgtHllType.HLL_4, 4);
    }

    public ThetaSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin) {
        this(keySelector, aggregateSelector, emitMin, TgtHllType.HLL_4, 4);
    }

    public ThetaSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin, TgtHllType tgtHllType, int lgk) {
        this.keySelector = keySelector;
        this.aggregateSelector = aggregateSelector;
        this.tgtHllType = tgtHllType;
        this.lgk = lgk;
    }


    @Override
    public void open(Configuration parameters) {
        if (aggregation == null) {
            aggregation = new HllSketchAggregation();
        }
    }

    @Override
    public void flatMap(T input, Collector<Tuple2<Object, HllSketchAggregation>> collector) throws Exception {
        Object key = keySelector.getKey(input);
        Object value = aggregateSelector.getKey(input);
        aggregation.update(key, value);
        System.out.println("Key: " + key + " " + aggregation);
        collector.collect(Tuple2.of(key, aggregation));
    }
}
