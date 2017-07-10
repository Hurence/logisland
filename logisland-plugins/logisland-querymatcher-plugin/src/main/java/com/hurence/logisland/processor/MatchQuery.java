/**
 * Copyright (C) 2016-2017 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;

import java.io.IOException;
import java.util.*;


@Tags({"analytic", "percolator", "record", "record", "query", "lucene"})
@CapabilityDescription("Query matching based on `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>`_\n\n" +
        "you can use this processor to handle custom events defined by lucene queries\n" +
        "a new record is added to output each time a registered query is matched\n\n" +
        "A query is expressed as a lucene query against a field like for example: \n\n" +
        ".. code::\n" +
        "\n" +
        "\tmessage:'bad exception'\n" +
        "\terror_count:[10 TO *]\n" +
        "\tbytes_out:5000\n" +
        "\tuser_name:tom*\n\n" +
        "Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations\n\n" +
        ".. warning::\n\n" +
        "\tdon't forget to set numeric fields property to handle correctly numeric ranges queries")
@DynamicProperty(name = "query", supportsExpressionLanguage = true, value = "some Lucene query", description = "generate a new record when this query is matched")
public class MatchQuery extends AbstractProcessor {

    public final static String ALERT_MATCH_NAME = "alert_match_name";
    public final static String ALERT_MATCH_QUERY = "alert_match_query";

    public static final PropertyDescriptor NUMERIC_FIELDS = new PropertyDescriptor.Builder()
            .name("numeric.fields")
            .description("a comma separated string of numeric field to be matched")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("output.record.type")
            .description("the output type of the record")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("alert_match")
            .build();

    public static final PropertyDescriptor ON_MISS_POLICY = new PropertyDescriptor.Builder()
            .name("policy.onmiss")
            .description("the policy applied to miss events: " +
                         "'" + OnMissPolicy.discard.toString() + "' (default value)" +
                             " drop events that did not match any query;" +
                         "'" + OnMissPolicy.forward.toString() + "'" +
                             " include also events that did not match any query.")
            .required(false)
            .addValidator(new StandardValidators.EnumValidator(OnMissPolicy.class))
            .defaultValue(OnMissPolicy.discard.toString())
            .build();

    public static final PropertyDescriptor ON_MATCH_POLICY = new PropertyDescriptor.Builder()
            .name("policy.onmatch")
            .description("the policy applied to match events: " +
                         "'" + OnMatchPolicy.first.toString() + "' (default value)" +
                         "'first' (default value) match events are tagged with the name and value of the first query " +
                             "that matched;" +

                         "'" + OnMatchPolicy.all.toString() + "' match events are tagged with all names and values of" +
                             " the queries that matched;")
            .required(false)
            .addValidator(new StandardValidators.EnumValidator(OnMatchPolicy.class))
            .defaultValue(OnMatchPolicy.first.toString())
            .build();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(NUMERIC_FIELDS);
        descriptors.add(OUTPUT_RECORD_TYPE);
        descriptors.add(ON_MATCH_POLICY);
        descriptors.add(ON_MISS_POLICY);
        descriptors.add(AbstractProcessor.INCLUDE_INPUT_RECORDS);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    private static Logger logger = LoggerFactory.getLogger(MatchQuery.class);

    /**
     * The policy that defines the behaviour when a record matches a query.
     */
    enum OnMatchPolicy {
        /**
         * Tag the matching records with only the first query name and query value. This is the default value.
         */
        first, /* legacy */
        /**
         * Tag the matching records with all query names and query values expressed as String arrays.
         */
        all
    }

    /**
     * The policy that defines the behaviour when a record did not match any query.
     */
    enum OnMissPolicy {
        /**
         * Discard non-matching records. Only tagged records are sent by this processor. This is the default value.
         */
        discard, /* legacy */
        /**
         * Forward also records.
         */
        forward
    }

    private Monitor monitor;
    private KeywordAnalyzer keywordAnalyzer;
    private StandardAnalyzer standardAnalyzer;
    private StopAnalyzer stopAnalyzer;
    private Map<String, MatchingRule> matchingRules;
    private OnMissPolicy onMissPolicy;
    private OnMatchPolicy onMatchPolicy;

    @Override
    public void init(final ProcessContext context) {


        keywordAnalyzer = new KeywordAnalyzer();
        standardAnalyzer = new StandardAnalyzer();
        stopAnalyzer = new StopAnalyzer();
        matchingRules = new HashMap<>();
        onMissPolicy = OnMissPolicy.valueOf(context.getPropertyValue(ON_MISS_POLICY).asString());
        onMatchPolicy = OnMatchPolicy.valueOf(context.getPropertyValue(ON_MATCH_POLICY).asString());
        NumericQueryParser queryMatcher = new NumericQueryParser("field");


        // loop over dynamic properties to add rules
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String name = entry.getKey().getName();
            final String query = entry.getValue();
            matchingRules.put(name, new MatchingRule(name, query));
        }

        try {
            monitor = new Monitor(queryMatcher, new TermFilteredPresearcher());

            // TODO infer numeric type here
            if (context.getPropertyValue(NUMERIC_FIELDS).isSet()) {
                final String[] numericFields = context.getPropertyValue(NUMERIC_FIELDS).asString().split(",");
                for (String numericField : numericFields) {
                    queryMatcher.setNumericField(numericField);
                }
            }


            //monitor = new Monitor(new LuceneQueryParser("field"), new TermFilteredPresearcher());
            for (MatchingRule rule : matchingRules.values()) {
                MonitorQuery mq = new MonitorQuery(rule.getName(), rule.getQuery());
                monitor.update(mq);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // may have not been initialized
        if (monitor == null)
            init(context);


        // convert all numeric fields to double to get numeric range working ...
        final List<Record> outRecords = new ArrayList<>();
        final List<InputDocument> inputDocs = new ArrayList<>();
        final Map<String, Record> inputRecords = new HashMap<>();
        for (final Record record : records) {
            final InputDocument.Builder docbuilder = InputDocument.builder(record.getId());
            for (final String fieldName : record.getAllFieldNames()) {

                switch (record.getField(fieldName).getType()) {
                    case STRING:
                        docbuilder.addField(fieldName, record.getField(fieldName).asString(), stopAnalyzer);
                        break;
                    case INT:
                        docbuilder.addField(new DoubleField(fieldName, record.getField(fieldName).asInteger(), Field.Store.YES));
                        break;
                    case LONG:
                        docbuilder.addField(new DoubleField(fieldName, record.getField(fieldName).asLong(), Field.Store.YES));
                        break;
                    case FLOAT:
                        docbuilder.addField(new DoubleField(fieldName, record.getField(fieldName).asFloat(), Field.Store.YES));
                        break;
                    case DOUBLE:
                        docbuilder.addField(new DoubleField(fieldName, record.getField(fieldName).asDouble(), Field.Store.YES));
                        break;
                    default:
                        docbuilder.addField(fieldName, record.getField(fieldName).asString(), keywordAnalyzer);
                }
            }

            inputDocs.add(docbuilder.build());
            inputRecords.put(record.getId(), record);
        }

        // match a batch of documents
        Matches<QueryMatch> matches;
        try {
            matches = monitor.match(DocumentBatch.of(inputDocs), SimpleMatcher.FACTORY);
        } catch (IOException e) {
            logger.error("Could not match documents", e);
            return outRecords;
        }

        MatchHandlers.MatchHandler _matchHandler = null;

        if (onMatchPolicy==OnMatchPolicy.first && onMissPolicy==OnMissPolicy.discard) {
            // Legacy behaviour
            _matchHandler = new MatchHandlers.LegacyMatchHandler();
        }
        else if (onMissPolicy==OnMissPolicy.discard) {
            // Ignore non matching records. Concat all query information (name, value) instead of first one only.
            _matchHandler = new MatchHandlers.ConcatMatchHandler();
        }
        else {
            // All records in, all records out. Concat all query information (name, value) instead of first one only.
            _matchHandler = new MatchHandlers.AllInAllOutMatchHandler(records, this.onMatchPolicy);
        }

        final MatchHandlers.MatchHandler matchHandler = _matchHandler;
        for (DocumentMatches<QueryMatch> docMatch : matches) {
            docMatch.getMatches().forEach(queryMatch ->
                matchHandler.handleMatch(inputRecords.get(docMatch.getDocId()),
                                         context,
                                         matchingRules.get(queryMatch.getQueryId()))
            );

        }

        return matchHandler.outputRecords();
    }
}
