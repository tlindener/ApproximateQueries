package de.lindener.analysis.amazon;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.lindener.analysis.Experiment;
import de.lindener.analysis.ExperimentType;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRating;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRatingSource;
import de.lindener.streaming.exact.queries.ExactFrequentItemsFunction;
import de.lindener.streaming.exact.queries.ExactFrequentItemsResult;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class AZFrequentItemsExact {
    public static void main(String... argv) throws Exception {
        AZFIArgs main = new AZFIArgs();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        run(main);
    }

    public static void run(AZFIArgs main) throws Exception {
        Experiment experiment = new Experiment();
        experiment.setType(ExperimentType.AR_FI_Exact);
        experiment.setSettings(main.toString());
        experiment.setStartTime(LocalDateTime.now());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ObjectMapper mapper = new ObjectMapper();

        DataStream<AmazonReviewRating> inputStream = env.addSource(new AmazonReviewRatingSource("/media/data/approximatequeries/ratings.csv", main.bound));
        KeySelector valueSelector = (KeySelector<AmazonReviewRating, String>) rating -> rating.getReviewerId();
        ExactFrequentItemsFunction exactFrequentItems = new ExactFrequentItemsFunction(valueSelector, 20, main.emitMin);
        inputStream.flatMap(exactFrequentItems).map(new MapFunction<ExactFrequentItemsResult, String>() {
            @Override
            public String map(ExactFrequentItemsResult exactFrequentItemsResult) throws Exception {

                return mapper.writeValueAsString(exactFrequentItemsResult);
            }
        }).writeAsText(experiment.getResultPath());

        JobExecutionResult result = env.execute();

        experiment.setEndTime(LocalDateTime.now());
        experiment.setRuntime(result.getNetRuntime(TimeUnit.SECONDS));
        experiment.storeExperiment();
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");
    }
}
