package de.lindener.streaming.approximate.queries;

import de.lindener.streaming.approximate.queries.functions.FrequentItemSketchFunction;
import de.lindener.streaming.approximate.queries.functions.QuantileFunction;
import de.lindener.streaming.approximate.queries.models.FrequentItemResult;
import de.lindener.streaming.approximate.queries.models.QuantileQueryResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Queries {

    public static <ITEM, KEY> DataStream<FrequentItemResult> continuousFrequentItems(DataStream<ITEM> inputStream, KeySelector valueSelector, int emitMin) {
        FrequentItemSketchFunction<ITEM, KEY> frequentItemSketchFunction = new FrequentItemSketchFunction(valueSelector, emitMin);
        return inputStream.flatMap(frequentItemSketchFunction);
    }

    public static <T> DataStream<QuantileQueryResult> continuousQuantiles(DataStream<T> inputStream, KeySelector valueSelector, int emitMin) {
        QuantileFunction quantileFunction = new QuantileFunction(valueSelector, emitMin);
        return inputStream.flatMap(quantileFunction);
    }
}
