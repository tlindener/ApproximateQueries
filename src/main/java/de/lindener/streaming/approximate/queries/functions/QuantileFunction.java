package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.quantiles.ItemsSketch;
import de.lindener.streaming.approximate.queries.models.QuantileQueryResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

public class QuantileFunction<T>
        extends RichFlatMapFunction<T, QuantileQueryResult<T>> {

    Logger LOG = LoggerFactory.getLogger(QuantileFunction.class);
    private ItemsSketch<T> sketch;
    private int sketchMapSize;
    private int emitMin;
    private int emitMinCounter = 0;
    private ErrorType errorType;
    KeySelector keySelector;
    private Comparator orderType;

    public QuantileFunction(KeySelector keySelector) {
        this(keySelector, 128, 100000, ErrorType.NO_FALSE_POSITIVES);
    }

    public QuantileFunction(KeySelector keySelector, int sketchMapSize, int emitMin) {
        this(keySelector, sketchMapSize, emitMin, ErrorType.NO_FALSE_POSITIVES);
    }

    public QuantileFunction(KeySelector keySelector, int sketchMapSize, int emitMin, ErrorType errorType) {
        this(keySelector, sketchMapSize, emitMin, errorType, Comparator.naturalOrder());
    }

    public QuantileFunction(KeySelector keySelector, int sketchMapSize, int emitMin, ErrorType errorType, Comparator orderType) {
        this.sketchMapSize = sketchMapSize;
        this.emitMin = emitMin;
        this.errorType = errorType;
        this.keySelector = keySelector;
        this.orderType = orderType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (sketch == null) {
            sketch = ItemsSketch.getInstance(sketchMapSize, (Comparator<? super T>) Comparator.naturalOrder());
            LOG.info("Created new sketch");
        }
    }

    @Override
    public void flatMap(T input, Collector<QuantileQueryResult<T>> collector) throws Exception {
        sketch.update(input);
        emitMinCounter++;
        if (emitMin > 0 && emitMinCounter == emitMin) {
            collector.collect(new QuantileQueryResult(sketch));
            emitMinCounter = 0;
        }
    }
}