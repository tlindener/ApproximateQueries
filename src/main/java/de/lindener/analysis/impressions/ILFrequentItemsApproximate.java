package de.lindener.analysis.impressions;

import com.beust.jcommander.JCommander;
import de.lindener.analysis.Experiment;
import de.lindener.analysis.ExperimentType;
import de.lindener.streaming.approximate.queries.Queries;
import de.lindener.streaming.approximate.queries.sources.adverts.ImpressionLog;
import de.lindener.streaming.approximate.queries.sources.adverts.RandomAdvertGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class ILFrequentItemsApproximate {

    public static void main(String... argv) throws Exception {
        ILFIArgs main = new ILFIArgs();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        run(main);
    }

    public static void run(ILFIArgs main) throws Exception {
        Experiment experiment = new Experiment();
        experiment.setType(ExperimentType.IL_FI_Approximate);
        experiment.setSettings(main.toString());
        experiment.setStartTime(LocalDateTime.now());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<ImpressionLog> inputStream = env.addSource(new RandomAdvertGenerator(main.bound, main.websites, main.cookies));
        KeySelector valueSelector = (KeySelector<ImpressionLog, String>) impressionLog -> impressionLog.getAdvertiser();
        Queries.continuousTopN(inputStream, valueSelector, main.top, main.emitMin).writeAsText(experiment.getResultPath());
        JobExecutionResult result = env.execute("Approximate Query - ImpressionLog");

        experiment.setEndTime(LocalDateTime.now());
        experiment.setRuntime(result.getNetRuntime(TimeUnit.SECONDS));
        experiment.storeExperiment();
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute");
    }
}
