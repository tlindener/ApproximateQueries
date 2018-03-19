package de.lindener.queries.streaming.approximate.models;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

import java.util.HashMap;
import java.util.Map;

public class HllAggregate {
    TgtHllType type;
    int lgk;
    HashMap<Object, HllSketch> sketchHashMap;

    public HllAggregate() {
        this.sketchHashMap = new HashMap<>();
    }

    public HllAggregate(TgtHllType type, int lgk, HashMap<Object, HllSketch> sketchHashMap) {
        this.type = type;
        this.lgk = lgk;
        this.sketchHashMap = sketchHashMap;
    }

    public TgtHllType getType() {
        return type;
    }

    public void setType(TgtHllType type) {
        this.type = type;
    }

    public int getLgk() {
        return lgk;
    }

    public void setLgk(int lgk) {
        this.lgk = lgk;
    }

    public HashMap<Object, HllSketch> getSketchHashMap() {
        return sketchHashMap;
    }

    public void setSketchHashMap(HashMap<Object, HllSketch> sketchHashMap) {
        this.sketchHashMap = sketchHashMap;
    }

    public static HashMap<Object, HllSketch> mergeHllSketchHashMap(HashMap<Object, HllSketch> smallMap, HashMap<Object, HllSketch> largeMap) {
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
