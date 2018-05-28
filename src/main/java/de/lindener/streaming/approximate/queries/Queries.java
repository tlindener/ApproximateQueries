package de.lindener.streaming.approximate.queries;

import de.lindener.streaming.approximate.queries.functions.FrequentItemSketchFunction;
import de.lindener.streaming.approximate.queries.functions.FrequentItemSketchStateless;
import de.lindener.streaming.approximate.queries.functions.QuantileFunction;
import de.lindener.streaming.approximate.queries.models.FrequentItemResult;
import de.lindener.streaming.approximate.queries.models.QuantileQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Queries {

    public static <ITEM, KEY> DataStream<FrequentItemResult> continuousFrequentItems(DataStream<ITEM> inputStream, KeySelector valueSelector, int emitMin, int mapSize) {
        FrequentItemSketchStateless<ITEM, KEY> frequentItemSketchFunction = new FrequentItemSketchStateless(valueSelector, emitMin, mapSize);
        return inputStream.keyBy(valueSelector).flatMap(frequentItemSketchFunction);
    }

    public static <T> DataStream<QuantileQueryResult> continuousQuantiles(DataStream<T> inputStream, KeySelector valueSelector, int emitMin) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector, emitMin);
        return inputStream.keyBy(valueSelector).flatMap(quantileFunction);
    }
}
