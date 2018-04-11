package de.lindener.streaming.queries.sources.adverts;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomAdvertGenerator implements SourceFunction<ImpressionLog> {
    private Random rnd = new Random();
    private int bound = 1000;
    private volatile boolean isRunning = true;
    private int counter = 0;
    private int NumPublishers = 5;
    private int NumAdvertisers = 3;
    int NumWebsites = 10000;
    int NumCookies = 10000;
    String[] publishers;
    String[] advertisers;
    String UnknownGeo = "unknown";
    String[] Geos = {"NY", "CA", "FL", "MI", "HI", UnknownGeo};


    public RandomAdvertGenerator(int bound) {
        this.bound = bound;
        publishers = createRandomList("publisher_", NumPublishers);
        advertisers = createRandomList("advertiser_", NumAdvertisers);
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
            long timestamp = System.currentTimeMillis();
            String publisher = publishers[rnd.nextInt(NumPublishers)];
            String advertiser = advertisers[rnd.nextInt(NumAdvertisers)];
            String website = "website_" + rnd.nextInt(NumWebsites) + ".com";
            String cookie = "cookie_" + rnd.nextInt(NumCookies);
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