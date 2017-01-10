/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
import com.hurence.logisland.record.StandardRecord;
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
        ".. code-block::\n\n" +
        "\tmessage:'bad exception'\n" +
        "\terror_count:[10 TO *]\n" +
        "\tbytes_out:5000\n" +
        "\tuser_name:tom*\n\n" +
        "Please read the `Lucene syntax guide <https://lucene.apache.org/core/5_5_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>`_ for supported operations\n\n" +
        ".. warning::\n\n" +
        "\tdon't forget to set numeric fields property to handle correctly numeric ranges queries")
@DynamicProperty(name = "query", supportsExpressionLanguage = true, value = "some Lucene query", description = "generate a new record when this query is matched")
public class MatchQuery extends AbstractProcessor {


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

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(NUMERIC_FIELDS);
        descriptors.add(OUTPUT_RECORD_TYPE);
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


    private Monitor monitor;
    private KeywordAnalyzer keywordAnalyzer;
    private StandardAnalyzer standardAnalyzer;
    private StopAnalyzer stopAnalyzer;
    private Map<String, MatchingRule> matchingRules;

    @Override
    public void init(final ProcessContext context) {


        keywordAnalyzer = new KeywordAnalyzer();
        standardAnalyzer = new StandardAnalyzer();
        stopAnalyzer = new StopAnalyzer();
        matchingRules = new HashMap<>();
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
        List<Record> outRecords = new ArrayList<>();
        List<InputDocument> inputDocs = new ArrayList<>();
        Map<String, Record> inputRecords = new HashMap<>();
        for (Record record : records) {
            InputDocument.Builder docbuilder = InputDocument.builder(record.getId());
            for (String fieldName : record.getAllFieldNames()) {

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
        Matches<QueryMatch> matches = null;
        try {
            matches = monitor.match(DocumentBatch.of(inputDocs), SimpleMatcher.FACTORY);
        } catch (IOException e) {
            logger.error("Could not match documents", e);
            return outRecords;
        }

        String outputRecordType = context.getPropertyValue(OUTPUT_RECORD_TYPE).asString();

        for (DocumentMatches<QueryMatch> docMatch : matches) {
            docMatch.getMatches().forEach(queryMatch -> {
                outRecords.add(
                        new StandardRecord(inputRecords.get(docMatch.getDocId()))
                                .setType(outputRecordType)
                                .setStringField("alert_match_name", queryMatch.getQueryId())
                                .setStringField("alert_match_query", matchingRules.get(queryMatch.getQueryId()).getQuery())
                );
            });

        }

        return outRecords;
    }


}
