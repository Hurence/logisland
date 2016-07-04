package com.hurence.logisland.processor;

import com.hurence.logisland.event.Event;
import com.sun.xml.internal.stream.buffer.AbstractProcessor;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;
import uk.co.flax.luwak.queryparsers.LuceneQueryParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by fprunier on 15/04/16.
 */
public class LuwakQueryMatcher extends AbstractEventProcessor {

    Logger logger = LoggerFactory.getLogger(LuwakQueryMatcher.class);


    private Monitor monitor = null;
    private KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
    private StandardAnalyzer standardAnalyzer = new StandardAnalyzer();



     void init(List<MatchingRule> rules) throws IOException {

        setRules(rules);

        monitor = new Monitor(new LuceneQueryParser("field"), new TermFilteredPresearcher());

        for (MatchingRule rule :getRules()) {
            MonitorQuery mq = new MonitorQuery(rule.getName(), rule.getQuery());
            monitor.update(mq);
        }
    }

    @Override
    public Collection<Event> process(ProcessContext context, Collection<Event> collection) {

        ArrayList<Event> outEvents = new ArrayList<>();

        ArrayList<InputDocument> docs = new ArrayList<>();
        for (Event ev : collection) {
            InputDocument.Builder docbuilder = InputDocument.builder(ev.getId());
            for (String fieldName : ev.keySet()) {
                if (ev.get(fieldName).getType().equalsIgnoreCase("string"))
                    docbuilder.addField(fieldName, ev.get(fieldName).getValue().toString(), standardAnalyzer);
                else
                    docbuilder.addField(fieldName, ev.get(fieldName).getValue().toString(), keywordAnalyzer);
            }

            docs.add(docbuilder.build());
        }

        // match a batch of documents
        Matches<QueryMatch> matches = null;
        try {
            matches = monitor.match(DocumentBatch.of(docs), SimpleMatcher.FACTORY);
        } catch (IOException e) {
            logger.error("Could not match documents", e);
            return outEvents;
        }

        for (DocumentMatches<QueryMatch> docMatch : matches) {
            Event outEv = new Event(EVENT_MATCH_TYPE_NAME);
            outEv.setId(docMatch.getDocId());
            // Only get last match for now, we should probably add them all
            for (QueryMatch queryMatch:docMatch.getMatches())
                outEv.put("match", "string", queryMatch.getQueryId());
            outEvents.add(outEv);
        }

        return outEvents;
    }

}
