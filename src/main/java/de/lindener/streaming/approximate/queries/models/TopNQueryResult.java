package de.lindener.streaming.approximate.queries.models;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;

import java.util.ArrayList;

public class TopNQueryResult<T> {
    public ArrayList<TopNQueryResultItem<T>> getResultList() {
        return resultList;
    }

    public void setResultList(ArrayList<TopNQueryResultItem<T>> resultList) {
        this.resultList = resultList;
    }

    ArrayList<TopNQueryResultItem<T>> resultList = new ArrayList<TopNQueryResultItem<T>>();

    public static <T> TopNQueryResult fromSketch(ItemsSketch<T> sketch, ErrorType errorType, int n) {
        TopNQueryResult result = new TopNQueryResult();
        ItemsSketch.Row<T>[] frequentItems = sketch.getFrequentItems(errorType);
        for (int i = 0; i < ((frequentItems.length >= n) ? n : frequentItems.length); i++) {
            result.getResultList().add(new TopNQueryResultItem<T>(frequentItems[i].getItem(), frequentItems[i].getEstimate(), frequentItems[i].getLowerBound(), frequentItems[i].getUpperBound()));
        }

        return result;
    }

    @Override
    public String toString() {
        return "TopNQueryResult{" +
                "resultList=" + resultList +
                '}';
    }
}
