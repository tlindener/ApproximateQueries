package de.lindener.streaming.exact.queries;

import de.lindener.streaming.approximate.queries.models.TopNQueryResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ExactFrequentItems<T> extends RichFlatMapFunction<T, Map<Object, Long>> {
    HashMap<Object, Long> frequentItems = new HashMap<>();
    KeySelector keySelector;
    int topN;
    public ExactFrequentItems(KeySelector keySelector, int topN){
        this.keySelector = keySelector;
        this.topN = topN;
    }

    @Override
    public void flatMap(T t, Collector<Map<Object, Long>> collector) throws Exception {
        Object value = keySelector.getKey(t);
        if(frequentItems.containsKey(value)){
            Long count = frequentItems.get(value);
            count++;
            frequentItems.put(value,count);
        }else{
            frequentItems.put(value, 1L);
        }

        Map<Object, Long> sortedMap =
                frequentItems.entrySet().stream()
                        .sorted(Entry.comparingByValue())
                        .limit(topN)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));

        collector.collect(sortedMap);
    }
}
