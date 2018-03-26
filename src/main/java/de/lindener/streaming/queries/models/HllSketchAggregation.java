package de.lindener.streaming.queries.models;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class HllSketchAggregation implements Serializable {
//    private Logger LOG = LoggerFactory.getLogger(HllSketchAggregation.class);

    private HashMap<Object, HllSketch> sketchMap = new HashMap<>();

    public void update(Object key, Object item) {
        HllSketch sketch;
        if (sketchMap.containsKey(key)) {
            sketch = sketchMap.get(key);
        } else {
            sketch = new HllSketch(4);
        }
        sketchMap.put(key, updateSketch(sketch, item));
    }

    private HllSketch updateSketch(HllSketch sketch, Object item) {
        if (item instanceof Integer) {
            int primitive = (Integer) item;
            sketch.update(primitive);
        } else if (item instanceof Long) {
            long primitive = (Long) item;
            sketch.update(primitive);
        } else if (item instanceof Double) {
            double primitive = (Double) item;
            sketch.update(primitive);
        } else if (item instanceof Long[]) {
            long[] primitive = ArrayUtils.toPrimitive((Long[]) item);
            sketch.update(primitive);
        } else if (item instanceof Integer[]) {
            int[] primitive = ArrayUtils.toPrimitive((Integer[]) item);
            sketch.update(primitive);
        } else if (item instanceof Character[]) {
            char[] primitive = ArrayUtils.toPrimitive((Character[]) item);
            sketch.update(primitive);
        } else if (item instanceof Byte[]) {
            byte[] primitive = ArrayUtils.toPrimitive((Byte[]) item);
            sketch.update(primitive);
        } else if (item instanceof String) {
            sketch.update((String) item);
        } else {
            throw new IllegalArgumentException("Input is of type " + item.getClass());
        }
        return sketch;
    }


    @Override
    public String toString() {
        return "HllSketchAggregation{" +
                "sketchMap=" + sketchMap.entrySet().stream().map(x -> {
            return x.getKey() + " " + x.getValue().getEstimate();
        }).collect(Collectors.toList()) +
                '}';
    }

    public HllSketchAggregation merge(HllSketchAggregation edgeValue) {
//        LOG.info("HllSketchAggregation");
        System.out.println("HllSketchAggregation - merge");
        this.sketchMap = mergeMaps(this.getSketchMap(), edgeValue.getSketchMap());
        return this;
    }

    private HashMap<Object, HllSketch> mergeMaps(HashMap<Object, HllSketch> smallMap, HashMap<Object, HllSketch> largeMap) {
        for (Map.Entry<Object, HllSketch> set : smallMap.entrySet()) {
            if (largeMap.containsKey(set.getKey())) {
                if (set.getValue().getEstimate() >= largeMap.get(set.getKey()).getEstimate()) {
                    largeMap.put(set.getKey(), set.getValue());
                }
            } else {
                largeMap.put(set.getKey(), set.getValue());
            }

        }
        return largeMap;
    }

    public HllSketchAggregation mergeValues(HllSketchAggregation edgeValue) {
//        LOG.info("HllSketchAggregation");
        System.out.println("HllSketchAggregation - mergeValues");
        this.sketchMap = mergeItems(this.getSketchMap(), edgeValue.getSketchMap());
        return this;
    }

    private HashMap<Object, HllSketch> mergeItems(HashMap<Object, HllSketch> smallMap, HashMap<Object, HllSketch> largeMap) {
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

    public HashMap<Object, HllSketch> getSketchMap() {
        return sketchMap;
    }

    public void setSketchMap(HashMap<Object, HllSketch> sketchMap) {
        this.sketchMap = sketchMap;
    }
}
