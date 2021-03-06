package de.lindener.analysis.wikitrace;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.lindener.analysis.Constants;
import de.lindener.analysis.Experiment;
import de.lindener.analysis.ExperimentType;
import de.lindener.analysis.amazon.AZFIArgs;
import de.lindener.streaming.approximate.queries.Queries;
import de.lindener.streaming.approximate.queries.models.FrequentItemResult;
import de.lindener.streaming.approximate.queries.sources.wikitrace.WikiTrace;
import de.lindener.streaming.approximate.queries.sources.wikitrace.WikiTraceSource;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class WTFrequentItemsApproximate {

    public static void main(String... argv) throws Exception {
        AZFIArgs main = new AZFIArgs();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        run(main);
    }

    public static void run(AZFIArgs main) throws Exception {
        Experiment experiment = new Experiment(ExperimentType.WT_FI_Approximate);
        experiment.setSettings(main.toString());
        experiment.setStartTime(LocalDateTime.now());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ObjectMapper mapper = new ObjectMapper();
        //mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        DataStream<WikiTrace> inputStream = env.addSource(new WikiTraceSource(Constants.ANALYSIS_WT_PATH, main.bound));
        KeySelector valueSelector = (KeySelector<WikiTrace, String>) rating -> rating.getUrl();
        Queries.continuousFrequentItems(inputStream, valueSelector, main.emitMin, main.mapSize).map(new MapFunction<FrequentItemResult, String>() {
            @Override
            public String map(FrequentItemResult exactFrequentItemsResult) throws Exception {

                return mapper.writeValueAsString(exactFrequentItemsResult);
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
