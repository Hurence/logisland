package com.hurence.logisland.processor;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.validator.StandardValidators;
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
import java.util.Collections;
import java.util.List;


public class QueryMatcherProcessor extends AbstractProcessor {

    static final long serialVersionUID = -1L;

    public static final PropertyDescriptor RULES = new PropertyDescriptor.Builder()
            .name("Luwak rules")
            .description("a comma separated string of rules")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("*")
            .build();


    private static Logger logger = LoggerFactory.getLogger(QueryMatcherProcessor.class);

    private static String EVENT_MATCH_TYPE_NAME = "querymatch";

    private Monitor monitor = null;
    private KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
    private StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
    private List<MatchingRule> matchingRules = Collections.emptyList();


    @Override
    public void init(final ProcessContext context) {


        final String rules = context.getProperty(RULES).asString();
        final String[] split = rules.split(",");
        for (int i = 0; i < split.length; i++) {
            matchingRules.add(new MatchingRule("rule" + i, split[i]));
        }

        try {
            monitor = new Monitor(new LuceneQueryParser("field"), new TermFilteredPresearcher());
            for (MatchingRule rule : matchingRules) {
                MonitorQuery mq = new MonitorQuery(rule.getName(), rule.getQuery());
                monitor.update(mq);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {


        List<Record> outRecords = new ArrayList<>();

        ArrayList<InputDocument> docs = new ArrayList<>();
        for (Record ev : records) {
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


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RULES);

        return Collections.unmodifiableList(descriptors);
    }


}
