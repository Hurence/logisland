package com.hurence.logisland.processor;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.StandardRecord;
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
public class LuwakQueryMatcher extends AbstractQueryMatcher {

    Logger logger = LoggerFactory.getLogger(LuwakQueryMatcher.class);


    private Monitor monitor = null;
    private KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
    private StandardAnalyzer standardAnalyzer = new StandardAnalyzer();


    public LuwakQueryMatcher init(List<MatchingRule> rules) throws IOException {

        setRules(rules);

        monitor = new Monitor(new LuceneQueryParser("field"), new TermFilteredPresearcher());

        for (MatchingRule rule : getRules()) {
            MonitorQuery mq = new MonitorQuery(rule.getName(), rule.getQuery());
            monitor.update(mq);
        }

        return this;
    }

    public Collection<StandardRecord> process(Collection<StandardRecord> collection) {

        ArrayList<StandardRecord> outRecords = new ArrayList<>();

        ArrayList<InputDocument> docs = new ArrayList<>();
        for (StandardRecord ev : collection) {
            InputDocument.Builder docbuilder = InputDocument.builder(ev.getId());
            for (String fieldName : ev.getAllFieldNames()) {
                if (ev.getField(fieldName).getType() == FieldType.STRING)
                    docbuilder.addField(fieldName, ev.getField(fieldName).getRawValue().toString(), standardAnalyzer);
                else
                    docbuilder.addField(fieldName, ev.getField(fieldName).getRawValue().toString(), keywordAnalyzer);
            }

            docs.add(docbuilder.build());
        }

        // match a batch of documents
        Matches<QueryMatch> matches = null;
        try {
            matches = monitor.match(DocumentBatch.of(docs), SimpleMatcher.FACTORY);
        } catch (IOException e) {
            logger.error("Could not match documents", e);
            return outRecords;
        }

        for (DocumentMatches<QueryMatch> docMatch : matches) {
            StandardRecord outEv = new StandardRecord(EVENT_MATCH_TYPE_NAME);
            outEv.setId(docMatch.getDocId());
            // Only getField last match for now, we should probably add them all
            for (QueryMatch queryMatch : docMatch.getMatches())
                outEv.setStringField("match", queryMatch.getQueryId());
            outRecords.add(outEv);
        }

        return outRecords;
    }

}
