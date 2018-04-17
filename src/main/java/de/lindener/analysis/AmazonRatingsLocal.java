package de.lindener.analysis;

import de.bytefish.jtinycsvparser.CsvParser;
import de.bytefish.jtinycsvparser.CsvParserOptions;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRating;
import de.lindener.streaming.approximate.queries.sources.amazon.AmazonReviewRatingsMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AmazonRatingsLocal {
    public static void main(String[] args){
        CsvParserOptions options = new CsvParserOptions(false, ",");
        // Create the Mapping:
        AmazonReviewRatingsMapper mapping = new AmazonReviewRatingsMapper(() -> new AmazonReviewRating());

        // Create the Parser:
        CsvParser<AmazonReviewRating> parser = new CsvParser<>(options, mapping);

        // Path to read from:
        Path csvFile = FileSystems.getDefault().getPath("C:\\Users\\tobias\\Desktop\\projects\\queries\\data", "item_dedup.csv");

        // Holds the Results:

        List<String> distinctElements;
        // Read the CSV File:
        try (Stream<CsvMappingResult<AmazonReviewRating>> stream = parser.readFromFile(csvFile, StandardCharsets.UTF_8)) {
            distinctElements = stream.map(p -> p.getResult()).filter(distinctByKey(p -> p.getAsin())).map(p -> p.getAsin()).collect(Collectors.toList());
        }
        for(String item : distinctElements){
            System.out.println(item);
        }


    }
    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor)
    {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
}
