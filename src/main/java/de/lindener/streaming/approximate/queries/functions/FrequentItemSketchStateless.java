package de.lindener.streaming.approximate.queries.functions;

import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;
import de.lindener.streaming.approximate.queries.models.FrequentItemResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

    public class FrequentItemSketchStateless<IN, OUT> extends RichFlatMapFunction<IN, FrequentItemResult> {
        Logger LOG = LoggerFactory.getLogger(FrequentItemSketchFunction.class);

        private ItemsSketch<OUT> sketch;
        private int sketchMapSize;
        private int emitMin;
        private int emitMinCounter = 0;
        private ErrorType errorType;
        private int topN;
        KeySelector<IN, OUT> keySelector;

        public FrequentItemSketchStateless(KeySelector<IN, OUT> keySelector, int emitMin) {
            this(keySelector, emitMin, 4096 * 16);
        }

        public FrequentItemSketchStateless(KeySelector<IN, OUT> keySelector, int emitMin, int sketchMapSize) {
            this(keySelector, emitMin, sketchMapSize, ErrorType.NO_FALSE_POSITIVES);
        }

        public FrequentItemSketchStateless(KeySelector<IN, OUT> keySelector, int emitMin, int sketchMapSize, ErrorType errorType) {
            this.keySelector = keySelector;
            this.topN = topN;
            this.sketchMapSize = sketchMapSize;
            this.errorType = errorType;
            this.emitMin = emitMin;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            LOG.info("Opened new ItemsSketchFunction");
            if (sketch == null) {
                sketch = new ItemsSketch<>(sketchMapSize);
                LOG.info("Created new sketch");
            }
        }

        @Override
        public void flatMap(IN t, Collector<FrequentItemResult> collector) throws Exception {
            OUT value = keySelector.getKey(t);
            sketch.update(value);
            emitMinCounter++;
            if (emitMin > 0 && emitMinCounter == emitMin) {
                collector.collect(FrequentItemResult.fromSketch(sketch, errorType));
                emitMinCounter = 0;
            }
        }

    }