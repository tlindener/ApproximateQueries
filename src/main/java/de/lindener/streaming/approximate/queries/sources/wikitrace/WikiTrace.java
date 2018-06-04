package de.lindener.streaming.approximate.queries.sources.wikitrace;

public class WikiTrace implements Comparable {
    private Integer counter;
    private String timestamp;
    private String url;
    private String dbUpdate;

    public String getDbUpdate() {
        return dbUpdate;
    }

    public void setDbUpdate(String dbUpdate) {
        this.dbUpdate = dbUpdate;
    }

    public Integer getCounter() {
        return counter;
    }

    public void setCounter(Integer counter) {
        this.counter = counter;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    @Override
    public int compareTo(Object o) {
        int base = this.getUrl().compareTo(((WikiTrace) o).getUrl());
        if (base == 0) {
            return this.getTimestamp().compareTo(((WikiTrace) o).getTimestamp());
        }
        return base;

    }

}
