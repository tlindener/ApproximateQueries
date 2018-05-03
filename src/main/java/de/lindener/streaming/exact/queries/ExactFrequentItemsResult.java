package de.lindener.streaming.exact.queries;

import java.util.Map;

public class ExactFrequentItemsResult {
    public ExactFrequentItemsResult(Map<Object, Long> frequentItems) {
        this.frequentItems = frequentItems;
    }

    public Map<Object, Long> getFrequentItems() {
        return frequentItems;
    }

    public void setFrequentItems(Map<Object, Long> frequentItems) {
        this.frequentItems = frequentItems;
    }

    private Map<Object, Long> frequentItems;
}
