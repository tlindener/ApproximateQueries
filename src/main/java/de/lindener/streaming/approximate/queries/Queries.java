package de.lindener.streaming.approximate.queries;

import de.lindener.streaming.approximate.queries.functions.QuantileFunction;
import de.lindener.streaming.approximate.queries.functions.TopNSketchFunction;
import de.lindener.streaming.approximate.queries.models.QuantileQueryResult;
import de.lindener.streaming.approximate.queries.models.TopNQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Queries {

    public static <ITEM, KEY> DataStream<TopNQueryResult> continuousTopN(DataStream<ITEM> inputStream, KeySelector valueSelector, int topN, int emitMin) {
        TopNSketchFunction<ITEM, KEY> topNSketchFunction = new TopNSketchFunction(valueSelector, topN, emitMin);
        return inputStream.flatMap(topNSketchFunction);
    }

    public static <T> DataStream<QuantileQueryResult> continuousQuantiles(DataStream<T> inputStream, KeySelector valueSelector, int emitMin) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector, emitMin);
        return inputStream.flatMap(quantileFunction);
    }
}
