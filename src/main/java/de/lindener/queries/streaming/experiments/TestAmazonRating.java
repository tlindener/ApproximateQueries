package de.lindener.queries.streaming.experiments;

import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRating;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRatingSource;
import de.lindener.streaming.exact.queries.ExactFrequentItemsFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TestAmazonRating {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<AmazonReviewRating> inputStream = env.addSource(new AmazonReviewRatingSource("C:\\Users\\tobias\\Desktop\\projects\\queries\\data\\ratings_Books.csv"));


        KeySelector targetKeySelector = new KeySelector<AmazonReviewRating, Object>() {
            @Override
            public Object getKey(AmazonReviewRating input) throws Exception {
                return input.getRating();
            }

        };
        KeySelector targetValueSelector = new KeySelector<AmazonReviewRating, Object>() {
            @Override
            public Object getKey(AmazonReviewRating input) throws Exception {
                return input.getAsin();
            }
        };

//        DataStream<HllSketchAggregation> stream = CountDistinctQueries.runContinuousHll(inputStream, targetKeySelector, targetValueSelector, 10000);
//        DataStream<QuantileQueryResult> stream1 = Queries.continuousQuantiles(inputStream, targetValueSelector);
//        stream1.map(new MapFunction<QuantileQueryResult, String>() {
//            @Override
//            public String map(QuantileQueryResult quantileQueryResult) throws Exception {
//                return quantileQueryResult.getSketch().toString();
//            }
//        }).print();
//
        ExactFrequentItemsFunction exactFrequentItems = new ExactFrequentItemsFunction(targetValueSelector, 500, 10000);
        inputStream.flatMap(exactFrequentItems);

//        Queries.continuousFrequentItems(inputStream,targetValueSelector,500);

        JobExecutionResult result = env.execute("My Flink Job");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " to execute");

    }
}
