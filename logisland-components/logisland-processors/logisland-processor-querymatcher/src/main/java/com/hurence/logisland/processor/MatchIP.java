/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.flax.luwak.*;
import uk.co.flax.luwak.matchers.SimpleMatcher;
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.net.util.SubnetUtils;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@Tags({"analytic", "percolator", "record", "record", "query", "lucene"})
@CapabilityDescription("IP address Query matching (using `Luwak <http://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/>)`_\n\n" +
        "You can use this processor to handle custom events matching IP address (CIDR)\n" +
        "The record sent from a matching an IP address record is tagged appropriately.\n\n" +
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
public class MatchIP extends MatchQuery {

    private HashMap<String, HashSet<Pair<String, Pattern>>> ipRegexps;
    private HashMap<String, MatchingRule> regexpMatchingRules;
    private final String ipSyntax = "^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d+$";
    private final String CIDRSyntax = "^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\/\\d+$";
    private final Pattern ipPattern = Pattern.compile(ipSyntax);
    private final Pattern cidrPattern = Pattern.compile(CIDRSyntax);
    private HashSet<String> luceneAttrsToQuery;

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

    @Override
    public void init(final ProcessContext context) {

        keywordAnalyzer = new KeywordAnalyzer();
        standardAnalyzer = new StandardAnalyzer();
        stopAnalyzer = new StopAnalyzer();
        matchingRules = new HashMap<>();
        regexpMatchingRules = new HashMap<>();
        onMissPolicy = OnMissPolicy.valueOf(context.getPropertyValue(ON_MISS_POLICY).asString());
        onMatchPolicy = OnMatchPolicy.valueOf(context.getPropertyValue(ON_MATCH_POLICY).asString());
        recordTypeUpdatePolicy = RecordTypeUpdatePolicy.valueOf(context.getPropertyValue(RECORD_TYPE_UPDATE_POLICY).asString());
        NumericQueryParser queryMatcher = new NumericQueryParser("field");
        luceneAttrsToQuery = new HashSet<String>();

        // loop over dynamic properties to add rules
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String name = entry.getKey().getName();
            final String query = entry.getValue();
            String[] params = query.split(":",2);
            if (params.length == 2){
                String queryField=params[0];
                String luceneQuery;
                String luceneValue;
                String ipValue = params[1];
                Matcher ipMatcher = ipPattern.matcher(ipValue);
                Matcher cidrMatcher = cidrPattern.matcher(ipValue);
                if (ipMatcher.lookingAt()){
                    // This is a static ip address
                    // convert it to a long
                    long addr = ipToLong(ipValue);
                    luceneValue = String.valueOf(addr);
                    luceneQuery=queryField+":"+luceneValue;
                    matchingRules.put(name, new MatchingRule(name, luceneQuery, query));
                    luceneAttrsToQuery.add(queryField);
                }
                else if (cidrMatcher.lookingAt()) {
                    // This is a cidr
                    // Convert it to a range
                    SubnetUtils su = new SubnetUtils(ipValue);
                    String lowIp = su.getInfo().getLowAddress();
                    String highIp = su.getInfo().getHighAddress();
                    long lowIpLong = ipToLong(lowIp);
                    long highIpLong = ipToLong(highIp);
                    luceneValue = "[ " + String.valueOf(lowIpLong) + " TO " + String.valueOf(highIpLong) + " ]";
                    luceneQuery=queryField+":"+luceneValue;
                    matchingRules.put(name, new MatchingRule(name, luceneQuery, query));
                    luceneAttrsToQuery.add(queryField);
                }
                else {
                    regexpMatchingRules.put(name, new MatchingRule(name, query));
                    // Consider the value to be a regexp
                    // To Be Done
                    Pattern ipRegexp = Pattern.compile(ipValue);
                    if (ipRegexps==null){
                        ipRegexps=new HashMap<>();
                    }
                    if (ipRegexps.containsKey(queryField)){
                        HashSet<Pair<String,Pattern>> regexpVals = ipRegexps.get(queryField);
                        regexpVals.add(new ImmutablePair<>(name, ipRegexp));
                        ipRegexps.put(queryField, regexpVals);
                    }
                    else {
                        HashSet<Pair<String, Pattern>> regexpVals = new HashSet<>();
                        regexpVals.add(new org.apache.commons.lang3.tuple.ImmutablePair<>(name, ipRegexp));
                        ipRegexps.put(queryField, regexpVals);
                    }
                }
            }
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
        } catch (IOException|UpdateException e) {
            getLogger().error("error while initializing Monitor", e);
        }


    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        // convert all numeric fields to double to get numeric range working ...
        final List<Record> outRecords = new ArrayList<>();
        final List<InputDocument> inputDocs = new ArrayList<>();
        final Map<String, Record> inputRecords = new HashMap<>();
        for (final Record record : records) {
            final InputDocument.Builder docbuilder = InputDocument.builder(record.getId());
            for (final String fieldName : record.getAllFieldNames()) {
                if (luceneAttrsToQuery.contains(fieldName) &&
                        record.getField(fieldName).getRawValue() != null) {
                    String ip = record.getField(fieldName).asString();
                    // Check the value is an IPv4 address or ignore
                    Matcher checkIp = ipPattern.matcher(ip);
                    if (checkIp.lookingAt()) {
                        long ipLong = ipToLong(ip);
                        docbuilder.addField(fieldName, String.valueOf(ipLong), keywordAnalyzer);
                    }
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
            getLogger().error("Could not match documents", e);
            return outRecords;
        }

        MatchHandlers.MatchHandler _matchHandler = null;


        if (onMatchPolicy== OnMatchPolicy.first && onMissPolicy== OnMissPolicy.discard) {
            // Legacy behaviour
            _matchHandler = new MatchHandlers.LegacyMatchHandler();
        }
        else if (onMissPolicy== OnMissPolicy.discard) {
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
                                         matchingRules.get(queryMatch.getQueryId()),
                                         recordTypeUpdatePolicy)
            );
        }
        if (ipRegexps != null && ipRegexps.size() > 0) {
            inputRecords.keySet().forEach(
                    (k) -> {
                        // Only consider records that have not matched any IP rules yet
                        int count = matches.getMatchCount(k);
                        if (count == 0) {
                            // Apply regexp rules if any available
                            for (String ipAttrName : ipRegexps.keySet()){
                                if (inputRecords.get(k).hasField(ipAttrName) &&
                                        (inputRecords.get(k).getField(ipAttrName).getRawValue() != null)) {
                                    HashSet<Pair<String, Pattern>> ipRegexHashset = ipRegexps.get(ipAttrName);
                                    ipRegexHashset.forEach(p -> {
                                        String ruleName = p.getLeft();
                                        Pattern ipRegexPattern = p.getRight();
                                        String attrValueToMatch = inputRecords.get(k).getField(ipAttrName).asString();
                                        Matcher ipMatcher = ipRegexPattern.matcher(attrValueToMatch);
                                        if (ipMatcher.lookingAt()){
                                            // This is a match !
                                            matchHandler.handleMatch(
                                                    inputRecords.get(k),
                                                    context,
                                                    regexpMatchingRules.get(ruleName),
                                                    recordTypeUpdatePolicy);
                                        }
                                    });
                                }
                            }
                        }
                    }
            );
        }
        return matchHandler.outputRecords();
    }

    public long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return result;
    }
}
