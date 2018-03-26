package de.lindener.streaming.queries.sources.amazon;

import de.bytefish.jtinycsvparser.builder.IObjectCreator;
import de.bytefish.jtinycsvparser.mapping.CsvMapping;

class AmazonReviewRatingsMapper extends CsvMapping<AmazonReviewRating> {
    public AmazonReviewRatingsMapper(IObjectCreator creator) {
        super(creator);
        mapProperty(0, String.class, AmazonReviewRating::setReviewerId);
        mapProperty(1, String.class, AmazonReviewRating::setAsin);
        mapProperty(2, Double.class, AmazonReviewRating::setRating);
        mapProperty(3, Long.class, AmazonReviewRating::setTimestamp);
    }

}
