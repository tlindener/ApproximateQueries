package de.lindener.queries.streaming.approximate.aggregate;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class HllSketchAggregateFunction implements ReduceFunction<HashMap<Object, HllSketch>> {
    Logger LOG = LoggerFactory.getLogger(HllSketchAggregateFunction.class);

    @Override
    public HashMap<Object, HllSketch> reduce(HashMap<Object, HllSketch> map1, HashMap<Object, HllSketch> map2) throws Exception {
        LOG.info("Map size is " +map1.keySet().size());
        for (Map.Entry<Object, HllSketch> set : map2.entrySet()) {
            if (map1.containsKey(set.getKey())) {
                Union union = new Union(4);
                union.update(set.getValue());
                union.update(map1.get(set.getKey()));
                map1.put(set.getKey(), union.getResult());
            } else {
                map1.put(set.getKey(), set.getValue());
            }

        }
        LOG.info("Map size is " +map1.keySet().size());
        return map1;
    }

    private HashMap<Object, HllSketch> mergeHllSketchHashMap(HashMap<Object, HllSketch> smallMap, HashMap<Object, HllSketch> largeMap) {
        for (Map.Entry<Object, HllSketch> set : smallMap.entrySet()) {
            if (largeMap.containsKey(set.getKey())) {
                Union union = new Union(4);
                union.update(set.getValue());
                union.update(largeMap.get(set.getKey()));
                largeMap.put(set.getKey(), union.getResult());
            } else {
                largeMap.put(set.getKey(), set.getValue());
            }

        }
        return largeMap;
    }
}
