package de.lindener.queries.streaming.approximate.functions;

import com.yahoo.sketches.quantiles.ItemsSketch;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

public class QuantileFunction<T>
        extends RichFlatMapFunction<T, ItemsSketch<T>>  {

    public static final String SKETCH_MAP_SIZE = "SketchMapSize";
    public static final String INTERMEDIATE_SUMMARY = "IntermediateSummary";

    Logger LOG = LoggerFactory.getLogger(QuantileFunction.class);
    private ListState<ItemsSketch<T>> checkpointedState;
    private ItemsSketch<T> sketch;
    private int sketchMapSize;
    private int intermediateSummary;
    private int intermediateSummaryCounter = 0;
    private Comparator orderType;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opened new ItemsSketchFunction");
        intermediateSummary = parameters.getInteger(INTERMEDIATE_SUMMARY, 100);
        orderType = Comparator.naturalOrder(); // .valueOf(parameters.getString(ERROR_TYPE, ErrorType.NO_FALSE_POSITIVES.name()));
        sketchMapSize = parameters.getInteger(SKETCH_MAP_SIZE,64);
        if (sketch == null) {
            sketch = ItemsSketch.getInstance(128, (Comparator<? super T>) Comparator.naturalOrder());
            LOG.info("Created new sketch");
        }
    }

    @Override
    public void flatMap(T input, Collector<ItemsSketch<T>> collector) throws Exception {
        sketch.update(input);
        intermediateSummaryCounter++;
        if (intermediateSummary > 0 && intermediateSummaryCounter == intermediateSummary) {
            collector.collect(sketch);
            intermediateSummaryCounter = 0;
        }
    }
}