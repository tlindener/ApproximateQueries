package de.lindener.streaming.approximate.queries.sources.amazon;

public class AmazonReviewRating implements Comparable {
    private String reviewerId;
    private String asin;
    private Double rating;
    private Long timestamp;

    public String getReviewerId() {
        return reviewerId;
    }

    public void setReviewerId(String reviewerId) {
        this.reviewerId = reviewerId;
    }

    public String getAsin() {
        return asin;
    }

    public void setAsin(String asin) {
        this.asin = asin;
    }

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(Object o) {
        int base = this.getAsin().compareTo(((AmazonReviewRating) o).getAsin());
        if (base == 0) {
            return this.getRating().compareTo(((AmazonReviewRating) o).getRating());
        }
        return base;

    }
}
