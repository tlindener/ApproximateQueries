package de.lindener.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import de.lindener.streaming.approximate.queries.sources.adverts.ImpressionLog;
import de.lindener.streaming.approximate.queries.sources.adverts.RandomAdvertGenerator;
import de.lindener.streaming.exact.queries.ExactFrequentItemsFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class ILFrequentItemsExact {
    public static void main(String... argv) throws Exception {
        ArgsFrequenItems main = new ArgsFrequenItems();
        JCommander.newBuilder()
                .addObject(main)
                .build()
                .parse(argv);
        run(main.bound, main.websites, main.cookies, main.top, main.emitMin);
    }

    public static void run(int bound, int websites, int cookies, int top, int emitMin) throws Exception {
        StringBuilder sb = new StringBuilder().append(Constants.ANALYSIS_RESULTS_BASE).append(Constants.ANALYSIS_PATH_IL_FREQUENT_ITEMS_EXACT).append(Calendar.getInstance().getTime().toString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        DataStream<ImpressionLog> inputStream = env.addSource(new RandomAdvertGenerator(bound, websites, cookies));

        KeySelector valueSelector = (KeySelector<ImpressionLog, String>) impressionLog -> impressionLog.getCookie();

        ExactFrequentItemsFunction exactFrequentItems = new ExactFrequentItemsFunction(valueSelector, top, emitMin);
        inputStream.flatMap(exactFrequentItems).writeAsText(sb.toString());

        JobExecutionResult result = env.execute("Exact Query - ImpressionLog");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " to execute");
    }
}
