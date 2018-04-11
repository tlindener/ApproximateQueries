package de.lindener.streaming.queries.processing;

import com.yahoo.sketches.frequencies.ErrorType;
import de.lindener.streaming.queries.functions.QuantileFunction;
import de.lindener.streaming.queries.functions.TopNSketchFunction;
import de.lindener.streaming.queries.functions.windows.TopNWindowAggregate;
import de.lindener.streaming.queries.models.QuantileQueryResult;
import de.lindener.streaming.queries.models.TopNQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

public class Queries {

    public static <T> DataStream<TopNQueryResult> continuousTopN(DataStream<T> inputStream, KeySelector valueSelector, int topN) {
        TopNSketchFunction topNSketchFunction = new TopNSketchFunction(valueSelector, topN);
        return inputStream.flatMap(topNSketchFunction);
    }

    public static <T> DataStream<TopNQueryResult> continuousTopN(DataStream<T> inputStream, KeySelector valueSelector, int topN, WindowAssigner window) {
        return Queries.continuousTopN(inputStream, valueSelector, topN, ErrorType.NO_FALSE_POSITIVES, window);
    }

    public static <T> DataStream<TopNQueryResult> continuousTopN(DataStream<T> inputStream, KeySelector valueSelector, int topN, ErrorType type, WindowAssigner window) {
        TopNWindowAggregate<T> aggregate = new TopNWindowAggregate<>(valueSelector, topN, type);
        return inputStream.keyBy(valueSelector).window(window).aggregate(aggregate);
    }

    public static DataStream<QuantileQueryResult> continuousQuantiles(DataStream<?> inputStream, KeySelector valueSelector) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector);
        return inputStream.flatMap(quantileFunction);
    }
}
