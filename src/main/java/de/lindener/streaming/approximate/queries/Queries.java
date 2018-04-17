package de.lindener.streaming.approximate.queries;

import de.lindener.streaming.approximate.queries.functions.QuantileFunction;
import de.lindener.streaming.approximate.queries.functions.TopNSketchFunction;
import de.lindener.streaming.approximate.queries.models.QuantileQueryResult;
import de.lindener.streaming.approximate.queries.models.TopNQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Queries {

    public static <T> DataStream<TopNQueryResult> continuousTopN(DataStream<T> inputStream, KeySelector valueSelector, int topN) {
        TopNSketchFunction topNSketchFunction = new TopNSketchFunction(valueSelector, topN);
        return inputStream.flatMap(topNSketchFunction);
    }

    public static <T> DataStream<QuantileQueryResult> continuousQuantiles(DataStream<T> inputStream, KeySelector valueSelector) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector);
        return inputStream.flatMap(quantileFunction);
    }
}
