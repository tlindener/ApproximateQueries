package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HllSketchFunction<T> extends RichFlatMapFunction<T, HllSketchAggregation> {
    Logger LOG = LoggerFactory.getLogger(HllSketchFunction.class);
    private KeySelector keySelector;
    private KeySelector aggregateSelector;
    private transient MapState<Object, HllSketch> hllSketchMapState;
    private TgtHllType tgtHllType;
    private int lgk;
    private int emitMin;
    private int emitMinCounter = 0;
    private String stateDescriptor;


    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector) {
        this(keySelector, aggregateSelector, 1, TgtHllType.HLL_4, 4);
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin) {
        this(keySelector, aggregateSelector, emitMin, TgtHllType.HLL_4, 4);
    }

    public HllSketchFunction(KeySelector keySelector, KeySelector aggregateSelector, int emitMin, String stateDescriptor) {
        this(keySelector, aggregateSelector, emitMin, TgtHllType.HLL_4, 4);
        this.stateDescriptor = stateDescriptor;
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
        this.stateDescriptor = stateDescriptor;
    }


    @Override
    public void open(Configuration parameters) {
        String METHOD_NAME = "open";
        LOG.info(METHOD_NAME);
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("HllMapState", Object.class, HllSketch.class);
        hllSketchMapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void flatMap(T input, Collector<HllSketchAggregation> collector) throws Exception {
        Object key = keySelector.getKey(input);
        Object value = aggregateSelector.getKey(input);
        if (hllSketchMapState.contains(key)) {
            HllSketch sketch = hllSketchMapState.get(key);
            sketch = updateSketch(sketch, value);
            hllSketchMapState.put(key, sketch);
        } else {
            HllSketch sketch = new HllSketch(lgk);
            sketch = updateSketch(sketch, value);
            hllSketchMapState.put(key, sketch);
        }
        emitMinCounter++;
        if (emitMin > 0 && emitMin == emitMinCounter) {
            // collector.collect(hllSketchMapState);
            emitMinCounter = 0;
        }
    }

    private HllSketch updateSketch(HllSketch sketch, Object item) {
        if (item instanceof Integer) {
            int primitive = (Integer) item;
            sketch.update(primitive);
        } else if (item instanceof Long) {
            long primitive = (Long) item;
            sketch.update(primitive);
        } else if (item instanceof Double) {
            double primitive = (Double) item;
            sketch.update(primitive);
        } else if (item instanceof Long[]) {
            long[] primitive = ArrayUtils.toPrimitive((Long[]) item);
            sketch.update(primitive);
        } else if (item instanceof Integer[]) {
            int[] primitive = ArrayUtils.toPrimitive((Integer[]) item);
            sketch.update(primitive);
        } else if (item instanceof Character[]) {
            char[] primitive = ArrayUtils.toPrimitive((Character[]) item);
            sketch.update(primitive);
        } else if (item instanceof Byte[]) {
            byte[] primitive = ArrayUtils.toPrimitive((Byte[]) item);
            sketch.update(primitive);
        } else if (item instanceof String) {
            sketch.update((String) item);
        } else {
            throw new IllegalArgumentException("Input is of type " + item.getClass());
        }
        return sketch;
    }

}
