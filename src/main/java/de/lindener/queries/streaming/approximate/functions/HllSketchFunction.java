package de.lindener.queries.streaming.approximate.functions;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HllSketchFunction<T extends Tuple>
        extends RichFlatMapFunction<T, Tuple2<Object, HllSketch>> {

    Logger LOG = LoggerFactory.getLogger(HllSketchFunction.class);
    private HllSketch sketch;
    private int intermediateSummary;
    private int intermediateSummaryCounter = 0;
    private TgtHllType tgtHllType;
    private int lgk = 4;
    private int fieldId;

    public HllSketchFunction() {
        this(0);
    }

    public HllSketchFunction(int fieldId) {
        this(fieldId, 100);
    }

    public HllSketchFunction(int fieldId, int emitMin) {
        this(fieldId, emitMin, TgtHllType.HLL_4, 4);
    }

    public HllSketchFunction(int fieldId, int emitMin, TgtHllType tgtHllType, int lgk) {
        if (lgk < 4 || lgk > 21) {
            throw new IllegalArgumentException();
        }
        this.lgk = lgk;
        this.tgtHllType = tgtHllType;
        this.intermediateSummary = emitMin;
        this.fieldId = fieldId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opened new ItemsSketchFunction");
        if (sketch == null) {
            sketch = new HllSketch(lgk);
            LOG.info("Created new sketch");
        }
    }

    @Override
    public void flatMap(T input, Collector<Tuple2<Object, HllSketch>> collector) throws Exception {
        _flatMap((Tuple) input, collector);
    }

    public void _flatMap(Tuple input, Collector<Tuple2<Object, HllSketch>> collector) {
        Object field = input.getField(fieldId);
        // HllSketch only supports limited inputs. And all inputs are expected as primitives
        if (field instanceof Integer) {
            int primitive = (Integer) field;
            sketch.update(primitive);
        } else if (field instanceof Long) {
            long primitive = (Long) field;
            sketch.update(primitive);
        } else if (field instanceof Double) {
            double primitive = (Double) field;
            sketch.update(primitive);
        } else if (field instanceof Long[]) {
            long[] primitive = ArrayUtils.toPrimitive((Long[]) field);
            sketch.update(primitive);
        } else if (field instanceof Integer[]) {
            int[] primitive = ArrayUtils.toPrimitive((Integer[]) field);
            sketch.update(primitive);
        } else if (field instanceof Character[]) {
            char[] primitive = ArrayUtils.toPrimitive((Character[]) field);
            sketch.update(primitive);
        } else if (field instanceof Byte[]) {
            byte[] primitive = ArrayUtils.toPrimitive((Byte[]) field);
            sketch.update(primitive);
        } else if (field instanceof String) {
            sketch.update((String) field);
        } else {
            throw new IllegalArgumentException("Input is of type " + field.getClass());
        }

        // Emit data only summarized (should reduce processing time)
        intermediateSummaryCounter++;
        if (intermediateSummary > 0 && intermediateSummaryCounter == intermediateSummary) {
            collector.collect(new Tuple2<>(field, sketch));
            intermediateSummaryCounter = 0;
        }
    }

}

