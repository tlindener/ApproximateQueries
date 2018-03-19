package de.lindener.queries.streaming.approximate.aggregate;

import com.yahoo.sketches.hll.HllSketch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class HllSketchFold implements SketchFold<Object, HllSketch, HashMap<Object, HllSketch>> {
    Logger LOG = LoggerFactory.getLogger(HllSketchFold.class);

    @Override
    public HashMap<Object, HllSketch> foldEdges(HashMap<Object, HllSketch> accum, Object streamKey, HllSketch edgeValue) throws Exception {
        LOG.info("foldEdges");
        accum.put(streamKey, edgeValue);
        return accum;
    }
}
