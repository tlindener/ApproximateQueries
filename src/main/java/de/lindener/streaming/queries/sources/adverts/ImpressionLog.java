package de.lindener.streaming.queries.sources.adverts;

import java.util.Date;

/*
 * Adapted from https://github.com/chimpler/blog-spark-streaming-log-aggregation
 * */
public class ImpressionLog {
    private Date timestamp;
    private String publisher;
    private String advertiser;
    private String website;
    private String geo;
    private Double bid;
    private String cookie;

    public ImpressionLog() {
    }


    public ImpressionLog(Date timestamp, String publisher, String advertiser, String website, String geo, Double bid, String cookie) {
        this.timestamp = timestamp;
        this.publisher = publisher;
        this.advertiser = advertiser;
        this.website = website;
        this.geo = geo;
        this.bid = bid;
        this.cookie = cookie;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getAdvertiser() {
        return advertiser;
    }

    public void setAdvertiser(String advertiser) {
        this.advertiser = advertiser;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }

    public Double getBid() {
        return bid;
    }

    public void setBid(Double bid) {
        this.bid = bid;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }
}
