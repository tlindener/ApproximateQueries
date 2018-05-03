package de.lindener.streaming.approximate.queries.sources.amazon;

import de.bytefish.jtinycsvparser.CsvParser;
import de.bytefish.jtinycsvparser.CsvParserOptions;
import de.bytefish.jtinycsvparser.tokenizer.StringSplitTokenizer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;

public class AmazonReviewRatingSource implements SourceFunction<AmazonReviewRating> {
    private volatile boolean isRunning = true;
    private String sourceFilePath;
    private int bound = 0;
    private int counter = 0;

    public AmazonReviewRatingSource(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }

    public AmazonReviewRatingSource(String sourceFilePath, int bound) {
        this.sourceFilePath = sourceFilePath;
        this.bound = bound;
    }

    @Override
    public void run(SourceContext<AmazonReviewRating> sourceContext) throws Exception {
        final Path csvSourcePath = FileSystems.getDefault().getPath(sourceFilePath);
        // Get the Stream of LocalWeatherData Elements in the CSV File:
        try (Stream<AmazonReviewRating> stream = AmazonReviewRatingsParser().readFromFile(csvSourcePath, StandardCharsets.UTF_8).filter(x -> x.isValid()).map(x -> x.getResult())) {

            // We need to get an iterator, since the SourceFunction has to break out of its main loop on cancellation:
            Iterator<AmazonReviewRating> iterator = stream.iterator();
//            if(bound != 0 && counter > bound){
//               cancel();
//            }
            // Make sure to cancel, when the Source function is canceled by an external event:
            while (isRunning && iterator.hasNext()) {
                sourceContext.collect(iterator.next());
                counter++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static CsvParser<AmazonReviewRating> AmazonReviewRatingsParser() {
        return new CsvParser<>(new CsvParserOptions(true, new StringSplitTokenizer(",", true)), new AmazonReviewRatingsMapper(() -> new AmazonReviewRating()));
    }


}
