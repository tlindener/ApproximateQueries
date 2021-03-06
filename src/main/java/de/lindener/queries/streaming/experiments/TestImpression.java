package de.lindener.queries.streaming.experiments;

import de.lindener.streaming.approximate.queries.sources.adverts.RandomAdvertGenerator;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import de.lindener.streaming.approximate.queries.CountDistinctQueries;
import de.lindener.streaming.approximate.queries.sources.adverts.ImpressionLog;
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
