package de.lindener.queries.streaming.approximate;

import com.yahoo.sketches.frequencies.ItemsSketch;
import com.yahoo.sketches.hll.HllSketch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerType(ItemsSketch.class);
//        env.registerType(HllSketch.class);
//        env.enableCheckpointing(10000);
        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        DataStreamSource<Tuple2<String, Integer>> inputStream = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private Random rnd = new Random();
            private int BOUND = 1000;
            private volatile boolean isRunning = true;
            private int counter = 0;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {


                while (isRunning && counter < BOUND) {
                    char c = (char) (rnd.nextInt(26) + 'a');
                    sourceContext.collect(new Tuple2<String, Integer>(String.valueOf(c), 1));
                    counter++;
                    Thread.sleep(5L);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        ApproximateQueries queries = new ApproximateQueries();
//        queries.continuousHllDistinctCount(inputStream, 0, 0, 100);
        DataStream<HashMap<Object, HllSketch>> stream = queries.continuousHllDistinctCountAggregate(inputStream, 0, 0, 100);
        stream.map(new MapFunction<HashMap<Object, HllSketch>, String>() {

            @Override
            public String map(HashMap<Object, HllSketch> objectHllSketchHashMap) throws Exception {
                return (String) objectHllSketchHashMap.keySet().toString();
            }
        }).print();
        env.execute();

    }
}
