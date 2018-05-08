package de.lindener.streaming.exact.queries;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ExactFrequentItemsFunction<T> extends RichFlatMapFunction<T, ExactFrequentItemsResult> {
    //HashMap<Object, Long> frequentItems = new HashMap<>();
    private transient MapState<Object, Long> frequentItems;
    KeySelector keySelector;
    int topN;
    private int emitMin;
    private int emitMinCounter = 0;

    public ExactFrequentItemsFunction(KeySelector keySelector, int topN, int emitMin) {
        this.keySelector = keySelector;
        this.topN = topN;
        this.emitMin = emitMin;
    }

    @Override
    public void flatMap(T t, Collector<ExactFrequentItemsResult> collector) throws Exception {

        Object value = keySelector.getKey(t);
        if (frequentItems.contains(value)) {
            Long count = frequentItems.get(value);
            count++;
            frequentItems.put(value, count);
        } else {
            frequentItems.put(value, 1L);
        }
        emitMinCounter++;
        if (emitMin > 0 && emitMinCounter == emitMin) {
            collector.collect(new ExactFrequentItemsResult(sortByValue(frequentItems, topN)));
            emitMinCounter = 0;
        }

    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(MapState<K, V> map, int limit) throws Exception {

        return StreamSupport.stream(map.entries().spliterator(), true)
                .sorted(Entry.<K, V>comparingByValue().reversed())
                .limit(limit)
                .collect(Collectors.toMap(
                        Entry::getKey,
                        Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}
