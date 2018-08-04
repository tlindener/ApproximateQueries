package de.lindener.analysis.amazon;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.lindener.analysis.Constants;
import de.lindener.analysis.Experiment;
import de.lindener.analysis.ExperimentType;
import de.lindener.streaming.approximate.queries.CountDistinctQueries;
import de.lindener.streaming.approximate.queries.models.HllSketchAggregation;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRating;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRatingSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class AZHllApproximate {
    public static void main(String... argv) throws Exception {
        AZFIArgs main = new AZFIArgs();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        run(main);
    }

    public static void run(AZFIArgs main) throws Exception {
        Experiment experiment = new Experiment(ExperimentType.AR_HLL_APPROXIMATE);
        experiment.setSettings(main.toString());
        experiment.setStartTime(LocalDateTime.now());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ObjectMapper mapper = new ObjectMapper();
        //mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        DataStream<AmazonReviewRating> inputStream = env.addSource(new AmazonReviewRatingSource(Constants.ANALYSIS_AZ_PATH_P, main.bound));
        KeySelector keySelector = (KeySelector<AmazonReviewRating, String>) rating -> rating.getAsin();
        KeySelector valueSelector = (KeySelector<AmazonReviewRating, String>) rating -> rating.getReviewerId();
        CountDistinctQueries.runContinuousHll(inputStream, keySelector, valueSelector, main.emitMin).map(new MapFunction<HllSketchAggregation, String>() {
            @Override
            public String map(HllSketchAggregation input) throws Exception {

                return mapper.writeValueAsString(input.getResult());
            }
        }).writeAsText(experiment.getResultPath());
        JobExecutionResult result = env.execute();

        experiment.setEndTime(LocalDateTime.now());
        experiment.setRuntime(result.getNetRuntime(TimeUnit.SECONDS));
        experiment.storeExperiment();
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");
        System.out.println(experiment.toString());
    }
}
