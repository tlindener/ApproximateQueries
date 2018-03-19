package de.lindener.queries.streaming.approximate.functions;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;
import de.lindener.queries.streaming.approximate.models.TopNQueryResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopNQueryFunction<T> extends RichFlatMapFunction<T, TopNQueryResult> {
    Logger LOG = LoggerFactory.getLogger(TopNQueryFunction.class);

    private ItemsSketch<T> sketch;
    private int sketchMapSize;
    private int intermediateSummary;
    private int intermediateSummaryCounter = 0;
    private ErrorType errorType;
    private int topN;
    public static final String SKETCH_MAP_SIZE = "SketchMapSize";
    public static final String INTERMEDIATE_SUMMARY = "IntermediateSummary";
    public static final String ERROR_TYPE = "ErrorType";
    public  static final String TOP_N = "Top_N";

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opened new ItemsSketchFunction");
        intermediateSummary = parameters.getInteger(INTERMEDIATE_SUMMARY, 100);
        errorType = ErrorType.valueOf(parameters.getString(ERROR_TYPE, ErrorType.NO_FALSE_POSITIVES.name()));
        sketchMapSize = parameters.getInteger(SKETCH_MAP_SIZE, 64);
        topN = parameters.getInteger(TOP_N,5);
        if (sketch == null) {
            sketch = new ItemsSketch<>(sketchMapSize);
            LOG.info("Created new sketch");
        }
    }

    @Override
    public void flatMap(T t, Collector<TopNQueryResult> collector) throws Exception {
        sketch.update(t);
        intermediateSummaryCounter++;
        if (intermediateSummary > 0 && intermediateSummaryCounter == intermediateSummary) {
            collector.collect(TopNQueryResult.fromSketch(sketch, errorType, topN));
            intermediateSummaryCounter = 0;
        }
    }

}