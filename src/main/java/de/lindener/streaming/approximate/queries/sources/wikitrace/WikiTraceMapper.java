package de.lindener.streaming.approximate.queries.sources.wikitrace;

import de.bytefish.jtinycsvparser.builder.IObjectCreator;
import de.bytefish.jtinycsvparser.mapping.CsvMapping;

public class WikiTraceMapper extends CsvMapping<WikiTrace> {
    public WikiTraceMapper(IObjectCreator creator) {
        super(creator);
        mapProperty(0, Integer.class, WikiTrace::setCounter);
        mapProperty(1, String.class, WikiTrace::setTimestamp);
        mapProperty(2, String.class, WikiTrace::setUrl);
        mapProperty(3, String.class, WikiTrace::setDbUpdate);
    }

}
