package de.lindener.queries.streaming.experiments;

import de.lindener.streaming.approximate.queries.functions.HllSketchFunction;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class TestDistributedMode {
    public static void main(String[] args) throws Exception {
        String[] categories = {"A", "B", "C", "D"};
        List<Tuple2<String, Integer>> data = new ArrayList<>();
        for (String category : categories) {
            for (int i = 1; i < 6; i++) {
                data.add(Tuple2.of(category, i));
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> inputStream = env.fromCollection(data);

        HllSketchFunction sketchFunction2 = new HllSketchFunction(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }

        }, new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f1;
            }
        });
        DataStream<HllSketchAggregation> stream = inputStream.flatMap(sketchFunction2);


//        WindowStreamTreeAggregation<HllSketchAggregation, HllSketchAggregation, HllSketchAggregation> aggregation = new WindowStreamTreeAggregation<HllSketchAggregation, HllSketchAggregation, HllSketchAggregation>(new SketchFold<HllSketchAggregation, HllSketchAggregation>() {
//            @Override
//            public HllSketchAggregation foldEdges(HllSketchAggregation accum, HllSketchAggregation edgeValue) throws Exception {
//                System.out.println("Merging in foldEdges " + accum + " with " + edgeValue);
//                HllSketchAggregation aggregate = accum.merge(edgeValue);
//                System.out.println(aggregate);
//                return aggregate;
//            }
//        }, new AggregationFunction<HllSketchAggregation>() {
//            @Override
//            public HllSketchAggregation reduce(HllSketchAggregation hllSketchAggregation, HllSketchAggregation t1) throws Exception {
//                System.out.println("Merging in reduce " + hllSketchAggregation + " with " + t1);
//                HllSketchAggregation aggregate = hllSketchAggregation.merge(t1);
//                System.out.println(aggregate);
//                return aggregate;
//            }
//        }, new HllSketchAggregation(), 1000, false);
//
//        DataStream<HllSketchAggregation> output = aggregation.run(stream);
//        env.execute();
//        Iterator<HllSketchAggregation> myOutput = DataStreamUtils.collect(output);
//        System.out.println("DataUtils" + myOutput.hasNext());
//        while (myOutput.hasNext()) {
//            HllSketchAggregation item = myOutput.next();
//            System.out.println(item);
//
//        }
    }
}

