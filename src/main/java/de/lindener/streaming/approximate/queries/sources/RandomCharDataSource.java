package de.lindener.streaming.approximate.queries.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomCharDataSource implements SourceFunction<Tuple2<String, Integer>> {
    private Random rnd = new Random();
    private int bound = 1000;
    private volatile boolean isRunning = true;
    private int counter = 0;

    public RandomCharDataSource(int bound) {
        this.bound = bound;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        while (isRunning && counter < bound) {
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
}
