package de.lindener.streaming.approximate.queries.examples;

import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestLocalMode {

    public static void main(String[] args) {
        String[] categories = {"A", "B", "C"};
        List<Tuple2<String, Integer>> data = new ArrayList<>();

        for (String category : categories) {
            for (int i = 1; i < 6; i++) {
                data.add(Tuple2.of(category, i));
            }
        }
        HllSketchAggregation sketch = new HllSketchAggregation();
        for (Tuple2<String, Integer> tuple : data) {
            sketch.update(tuple.f0, 1);
        }
        System.out.println(sketch);
    }

}
