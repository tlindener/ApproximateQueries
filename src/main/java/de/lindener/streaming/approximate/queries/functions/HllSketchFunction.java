package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.hll.TgtHllType;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HllSketchFunction<T> extends RichFlatMapFunction<T, HllSketchAggregation> {
    Logger LOG = LoggerFactory.getLogger(HllSketchFunction.class);
    private KeySelector keySelector;
    private KeySelector aggregateSelector;
    private transient ValueState<HllSketchAggregation> sketchValueState;
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

        ValueStateDescriptor<HllSketchAggregation> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<HllSketchAggregation>() {
                        }), // type information
                        new HllSketchAggregation(tgtHllType, lgk)); // default value of the state, if nothing was set
        sketchValueState = getRuntimeContext().getState(descriptor);



    }

    @Override
    public void flatMap(T input, Collector<HllSketchAggregation> collector) throws Exception {
        HllSketchAggregation aggregation = sketchValueState.value();
        Object key = keySelector.getKey(input);
        Object value = aggregateSelector.getKey(input);
        aggregation.update(key, value);
        sketchValueState.update(aggregation);
        emitMinCounter++;
        if (emitMin > 0 && emitMin == emitMinCounter) {
            collector.collect(aggregation);
            emitMinCounter = 0;
        }
    }
}
