package de.lindener.queries.streaming.experiments;

import de.lindener.streaming.approximate.queries.CountDistinctQueries;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SmallDataExampleWindowed {
    public static void main(String[] args) throws Exception {
        String[] categories = {"A", "B"};
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (String category : categories) {
            for (int i = 1; i < 6; i++) {
                data.add(Tuple2.of(category, i));
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
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
        DataStream<HllSketchAggregation> stream = CountDistinctQueries.runContinuousWindowHll(inputStream, targetKeySelector, targetValueSelector, TumblingProcessingTimeWindows.of(Time.seconds(1)));
        stream.map(new MapFunction<HllSketchAggregation, String>() {
            @Override
            public String map(HllSketchAggregation hllSketchAggregation) throws Exception {
                System.out.println(hllSketchAggregation.toString());
                return hllSketchAggregation.toString();
            }
        }).print();
        env.execute();
    }
}
