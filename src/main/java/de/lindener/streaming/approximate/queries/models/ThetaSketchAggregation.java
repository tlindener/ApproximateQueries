package de.lindener.streaming.approximate.queries.models;

import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ThetaSketchAggregation implements Serializable {
//    private Logger LOG = LoggerFactory.getLogger(HllSketchAggregation.class);

    private HashMap<Object, UpdateSketch> sketchMap = new HashMap<>();

    public void update(Object key, Object item) {
        UpdateSketch sketch;
        if (sketchMap.containsKey(key)) {
            sketch = sketchMap.get(key);
        } else {
            sketch = UpdateSketch.builder().build();
        }
        sketchMap.put(key, updateSketch(sketch, item));
    }

    private UpdateSketch updateSketch(UpdateSketch sketch, Object item) {
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

    public ThetaSketchAggregation merge(ThetaSketchAggregation edgeValue) {
//        LOG.info("HllSketchAggregation");
        System.out.println("HllSketchAggregation - merge");
        this.sketchMap = mergeMaps(this.getSketchMap(), edgeValue.getSketchMap());
        return this;
    }

    private HashMap<Object, UpdateSketch> mergeMaps(HashMap<Object, UpdateSketch> smallMap, HashMap<Object, UpdateSketch> largeMap) {
        for (Map.Entry<Object, UpdateSketch> set : smallMap.entrySet()) {
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


    public HashMap<Object, UpdateSketch> getSketchMap() {
        return sketchMap;
    }

    public void setSketchMap(HashMap<Object, UpdateSketch> sketchMap) {
        this.sketchMap = sketchMap;
    }
}
