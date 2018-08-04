package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.hll.TgtHllType;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HllSketchFunction<T> extends RichFlatMapFunction<T, HllSketchAggregation> {
    Logger LOG = LoggerFactory.getLogger(HllSketchFunction.class);
    private KeySelector keySelector;
    private KeySelector aggregateSelector;
    private HllSketchAggregation aggregation;
    private TgtHllType tgtHllType;
    private int lgk;
    private int emitMin;
    private int emitMinCounter = 0;

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector) {
        this(keySelector, aggregateSelector, 1, TgtHllType.HLL_4, 4);
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin) {
        this(keySelector, aggregateSelector, emitMin, TgtHllType.HLL_4, 4);
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin, String stateDescriptor) {
        this(keySelector, aggregateSelector, emitMin, TgtHllType.HLL_4, 4);
//        this.stateDescriptor = stateDescriptor;
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin, TgtHllType tgtHllType, int lgk) {
        this.keySelector = keySelector;
        this.aggregateSelector = aggregateSelector;
        this.tgtHllType = tgtHllType;
        this.lgk = lgk;
        this.emitMin = emitMin;
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin, TgtHllType tgtHllType, int lgk, String stateDescriptor) {
        this.keySelector = keySelector;
        this.aggregateSelector = aggregateSelector;
        this.tgtHllType = tgtHllType;
        this.lgk = lgk;
        this.emitMin = emitMin;
//        this.stateDescriptor = stateDescriptor;
    }


    @Override
    public void open(Configuration parameters) {
        String METHOD_NAME = "open";
        LOG.info(METHOD_NAME);
        if (aggregation == null) {
            aggregation = new HllSketchAggregation(tgtHllType, lgk);
        }
    }

    @Override
    public void flatMap(T input, Collector<HllSketchAggregation> collector) throws Exception {
        Object key = keySelector.getKey(input);
        Object value = aggregateSelector.getKey(input);
        aggregation.update(key, value);
        emitMinCounter++;
        if (emitMin > 0 && emitMin == emitMinCounter) {
            collector.collect(aggregation);
            emitMinCounter = 0;
        }
    }
}