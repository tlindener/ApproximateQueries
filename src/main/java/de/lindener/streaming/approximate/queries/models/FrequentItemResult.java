package de.lindener.streaming.approximate.queries.models;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

import java.util.ArrayList;

public class FrequentItemResult<T> {
    public ArrayList<FrequentItemResultItem<T>> getResultList() {
        return resultList;
    }

    public void setResultList(ArrayList<FrequentItemResultItem<T>> resultList) {
        this.resultList = resultList;
    }

    ArrayList<FrequentItemResultItem<T>> resultList = new ArrayList<FrequentItemResultItem<T>>();

    public static <T> FrequentItemResult fromSketch(ItemsSketch<T> sketch, ErrorType errorType) {
        FrequentItemResult result = new FrequentItemResult();
        ItemsSketch.Row<T>[] frequentItems = sketch.getFrequentItems(errorType);
        for (int i = 0; i < frequentItems.length; i++) {
            result.getResultList().add(new FrequentItemResultItem<T>(frequentItems[i].getItem(), frequentItems[i].getEstimate(), frequentItems[i].getLowerBound(), frequentItems[i].getUpperBound()));
        }

        return result;
    }

    @Override
    public String toString() {
        return "FrequentItemResult{" +
                "resultList=" + resultList +
                '}';
    }
}
