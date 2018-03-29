package de.lindener.streaming.queries.examples;

import de.lindener.streaming.queries.models.HllSketchAggregation;
import de.lindener.streaming.queries.processing.CountDistinctQueries;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SmallDataExample {
    public static void main(String[] args) throws Exception {
        String[] categories = {"A", "B"};
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (String category : categories) {
            for (int i = 1; i < 6; i++) {
                data.add(Tuple2.of(category, i));
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> inputStream = env.fromCollection(data);

        KeySelector targetKeySelector = new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }

        };
        KeySelector targetValueSelector = new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f1;
            }

        };

        DataStream<HllSketchAggregation> stream = CountDistinctQueries.runContinuousHll(inputStream, targetKeySelector, targetValueSelector, 1);
        env.execute();
    }
}
