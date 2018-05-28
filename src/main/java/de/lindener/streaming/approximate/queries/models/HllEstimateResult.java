package de.lindener.streaming.approximate.queries.models;

import java.util.List;

public class HllEstimateResult {

    private List<HllEstimate> resultList;

    public HllEstimateResult() {
    }

    public HllEstimateResult(List<HllEstimate> resultList) {
        this.resultList = resultList;
    }

    public List<HllEstimate> getResultList() {
        return resultList;
    }

    public void setResultList(List<HllEstimate> resultList) {
        this.resultList = resultList;
    }
}
