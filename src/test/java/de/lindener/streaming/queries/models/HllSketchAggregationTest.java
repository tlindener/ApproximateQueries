package de.lindener.streaming.queries.models;


import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class HllSketchAggregationTest {

    @Test
    public void update() {
        HllSketchAggregation aggregation = new HllSketchAggregation();
        aggregation.update("A", 1);
        aggregation.update("A", 2);
        aggregation.update("A", 3);
        aggregation.update("B", 2);
        aggregation.update("B", 3);
        Set<Object> value = aggregation.getSketchMap().keySet();
        Assert.assertNotNull(value);
        Assert.assertTrue(value.contains("A"));
        Assert.assertTrue(value.contains("B"));
        Assert.assertEquals(3, (int) Math.floor((double) aggregation.getSketchMap().get("A").getEstimate()));
        Assert.assertEquals(2, (int) Math.floor((double) aggregation.getSketchMap().get("B").getEstimate()));
    }

    @Test
    public void merge() {
        HllSketchAggregation aggregation = new HllSketchAggregation();
        aggregation.update("A", 1);
        aggregation.update("A", 2);
        aggregation.update("A", 3);
        aggregation.update("B", 1);
        aggregation.update("B", 2);

        HllSketchAggregation aggregation2 = new HllSketchAggregation();
        aggregation2.update("B", 3);
        aggregation2.update("B", 4);
        aggregation2.update("B", 5);
        aggregation2.update("C", 2);
        aggregation2.update("C", 3);

        HllSketchAggregation aggregation3 = aggregation.merge(aggregation2);
        Set<Object> value = aggregation3.getSketchMap().keySet();
        Assert.assertNotNull(value);
        Assert.assertTrue(value.contains("A"));
        Assert.assertTrue(value.contains("B"));
        Assert.assertTrue(value.contains("C"));
        Assert.assertEquals(3, (int) Math.floor((double) aggregation.getSketchMap().get("A").getEstimate()));
        Assert.assertEquals(5, (int) Math.floor((double) aggregation.getSketchMap().get("B").getEstimate()));
        Assert.assertEquals(2, (int) Math.floor((double) aggregation.getSketchMap().get("C").getEstimate()));
    }


}