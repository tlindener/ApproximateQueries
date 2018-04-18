package de.lindener.streaming.approximate.queries.sources.adverts;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class RandomAdvertGenerator implements SourceFunction<ImpressionLog> {
    private Random rnd = new Random();
    private int bound = 1000;
    private volatile boolean isRunning = true;
    private int counter = 0;
    private int NumPublishers = 5;
    private int NumAdvertisers = 3;
    int numWebsites = 10000;
    int numCookies = 10000;
    String[] publishers;
    String[] advertisers;
    String UnknownGeo = "unknown";
    String[] Geos = {"NY", "CA", "FL", "MI", "HI", UnknownGeo};


    public RandomAdvertGenerator(int bound) {
        this(bound, 10000, 10000, 5, 3);
    }

    public RandomAdvertGenerator(int bound, int numWebsites, int numCookies) {
        this(bound, numWebsites, numCookies, 5, 3);
    }

    public RandomAdvertGenerator(int bound, int numWebsites, int numCookies, int numPublishers, int numAdvertisers) {
        this.bound = bound;
        this.numWebsites = numWebsites;
        this.numCookies = numCookies;
        publishers = createRandomList("publisher_", numPublishers);
        advertisers = createRandomList("advertiser_", numAdvertisers);
    }

    private String[] createRandomList(String name, int max) {
        String[] items = new String[max];
        for (int i = 0; i < max; i++) {
            items[i] = name + i;
        }
        return items;
    }

    @Override
    public void run(SourceContext<ImpressionLog> sourceContext) throws Exception {
        while (isRunning && counter < bound) {
            Date timestamp = Calendar.getInstance().getTime();
            String publisher = publishers[rnd.nextInt(NumPublishers)];
            String advertiser = advertisers[rnd.nextInt(NumAdvertisers)];
            String website = "website_" + rnd.nextInt(numWebsites) + ".com";
            String cookie = "cookie_" + rnd.nextInt(numCookies);
            String geo = Geos[rnd.nextInt(Geos.length)];
            Double bid = Math.abs(rnd.nextDouble()) % 1;
            ImpressionLog log = new ImpressionLog(timestamp, publisher, advertiser, website, geo, bid, cookie);
            sourceContext.collect(log);
            counter++;

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}