package de.lindener.streaming.queries.processing;

import de.lindener.streaming.queries.functions.QuantileFunction;
import de.lindener.streaming.queries.functions.TopNSketchFunction;
import de.lindener.streaming.queries.models.QuantileQueryResult;
import de.lindener.streaming.queries.models.TopNQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Queries {

    public static DataStream<TopNQueryResult> continuousTopN(DataStream<?> inputStream, KeySelector valueSelector, int topN) {
        TopNSketchFunction topNSketchFunction = new TopNSketchFunction(valueSelector, topN);
        return inputStream.flatMap(topNSketchFunction);
    }

    public static DataStream<QuantileQueryResult> continuousQuantiles(DataStream<?> inputStream, KeySelector valueSelector, int topN) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector);
        return inputStream.flatMap(quantileFunction);
    }
}
