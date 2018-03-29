package de.lindener.streaming.queries.examples;

import de.lindener.streaming.queries.models.HllSketchAggregation;
import de.lindener.streaming.queries.processing.CountDistinctQueries;
import de.lindener.streaming.queries.sources.adverts.ImpressionLog;
import de.lindener.streaming.queries.sources.adverts.RandomAdvertGenerator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestImpression {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ImpressionLog> inputStream = env.addSource(new RandomAdvertGenerator(1000000));

        KeySelector keySelector = new KeySelector<ImpressionLog, String>() {
            @Override
            public String getKey(ImpressionLog impressionLog) throws Exception {
                return impressionLog.getAdvertiser() + " - " + impressionLog.getGeo();
            }

        };

        KeySelector valueSelector = new KeySelector<ImpressionLog, String>() {
            @Override
            public String getKey(ImpressionLog impressionLog) throws Exception {
                return impressionLog.getCookie();
            }

        };
        DataStream<HllSketchAggregation> outputStream = CountDistinctQueries.runContinuousHll(inputStream, keySelector, valueSelector, 1000);
        outputStream.print();
        env.execute();
    }
}
