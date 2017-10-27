/**
 * Copyright (C) 2017 Hurence (support@hurence.com)
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

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;

import java.util.Collection;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.hurence.logisland.processor.MatchQuery.ALERT_MATCH_NAME;
import static com.hurence.logisland.processor.MatchQuery.ALERT_MATCH_QUERY;

/**
 * This class provides various implementations of MatchHandler that handle record match against lucene query.
 */
class MatchHandlers {

    private MatchHandlers() {} /* prevent instantiation */

    /**
     * This interface defines an matchHandler that handles the processing when a query matches. All processed records are
     * collected internally and should be reclaimed when all query matches have been processed.
     */
    interface MatchHandler
    {
        /**
         * Processes a query match.
         * @param record the record that matched the query.
         * @param context the context of the processor.
         * @param matchingRule the name and value of the matched query.
         */
        void handleMatch(Record record,
                         ProcessContext context,
                         MatchingRule matchingRule,
                         MatchQuery.RecordTypeUpdatePolicy recordTypeUpdatePolicy);

        /**
         * Returns the processed records.
         *
         * @return the processed records.
         */
        Collection<Record> outputRecords();
    }

    /**
     * This executors create a new record for each matching query.
     */
    static class LegacyMatchHandler
           implements MatchHandler
    {
        // All the processed records.
        private Collection<Record> outRecords = new ArrayList<>();

        @Override
        public void handleMatch(final Record record,
                                final ProcessContext context,
                                final MatchingRule matchingRule,
                                MatchQuery.RecordTypeUpdatePolicy recordTypeUpdatePolicy) {
            this.outRecords.add(
                    new StandardRecord(record)
                            .setType(
                                (recordTypeUpdatePolicy == MatchQuery.RecordTypeUpdatePolicy.overwrite) ?
                                        context.getPropertyValue(MatchQuery.OUTPUT_RECORD_TYPE).asString() : record.getType()
                            )
                            .setStringField(ALERT_MATCH_NAME, matchingRule.getName())
                            .setStringField(ALERT_MATCH_QUERY, matchingRule.getLegacyQuery()));
        }

        @Override
        public Collection<Record> outputRecords() {
            return this.outRecords;
        }
    }

    /**
     * This executors concatenate all matching query names and all matching query values as arrays without duplicating
     * processed records from their identifiers standpoint.
     */
    static class ConcatMatchHandler
           implements MatchHandler
    {
        // A map that handles all processed records.
        private Map<String/*recordIds*/, Record> outRecords = new HashMap<>();

        @Override
        public void handleMatch(final Record record,
                                final ProcessContext context,
                                final MatchingRule matchingRule,
                                MatchQuery.RecordTypeUpdatePolicy recordTypeUpdatePolicy) {
            com.hurence.logisland.record.Field nameField = null;
            com.hurence.logisland.record.Field queryField = null;

            Record outRecord = this.outRecords.get(record.getId());

            if ( outRecord == null ) {
                // First match. Clone record and set query information.
                outRecord = new StandardRecord(record)
                        .setType(
                                (recordTypeUpdatePolicy == MatchQuery.RecordTypeUpdatePolicy.overwrite) ?
                                        context.getPropertyValue(MatchQuery.OUTPUT_RECORD_TYPE).asString() : record.getType()
                        );
                this.outRecords.put(record.getId(), outRecord);

                // Keep all matched query information.
                nameField = new com.hurence.logisland.record.Field(ALERT_MATCH_NAME, FieldType.ARRAY, new String[]{matchingRule.getName()});
                queryField = new com.hurence.logisland.record.Field(ALERT_MATCH_QUERY, FieldType.ARRAY, new String[]{matchingRule.getLegacyQuery()});
            }
            else {
                // Append query information to existing one(s).
                String[] names = (String[])outRecord.getField(ALERT_MATCH_NAME).getRawValue();
                names = Arrays.copyOf(names, names.length+1);
                names[names.length-1] = matchingRule.getName();
                nameField = new com.hurence.logisland.record.Field(ALERT_MATCH_NAME, FieldType.ARRAY, names);

                String[] queries = (String[])outRecord.getField(ALERT_MATCH_QUERY).getRawValue();
                queries = Arrays.copyOf(queries, queries.length+1);
                queries[names.length-1] = matchingRule.getLegacyQuery();
                queryField = new com.hurence.logisland.record.Field(ALERT_MATCH_QUERY, FieldType.ARRAY, queries);
            }

            // Set or update fields.
            outRecord.setField(nameField);
            outRecord.setField(queryField);
        }

        @Override
        public Collection<Record> outputRecords() {
            return new ArrayList(this.outRecords.values());
        }
    }

    /**
     * This executors ensures that non-matching records received by the processor are also sent out.
     */
    static class AllInAllOutMatchHandler
           implements MatchHandler
    {
        // In case policy is 'forward', records that did not match any document must be sent to output anyway.
        private final Map<String/*recordIds*/, Record> inputRecords;

        // The inner matchHandler that either keeps the first matching rule information or concatenate all matching rules.
        private final MatchHandler matchHandler;

        /**
         *
         * @param records the records received by the processor.
         * @param onMatchPolicy the match policy to specify if only the first matching rule or all matching rules
         *                      are tagged in the send out records.
         */
        public AllInAllOutMatchHandler(final Collection<Record> records,
                                       final MatchQuery.OnMatchPolicy onMatchPolicy) {
            this.matchHandler = onMatchPolicy== MatchQuery.OnMatchPolicy.first ? new LegacyMatchHandler()
                                        : new ConcatMatchHandler();

            this.inputRecords = new HashMap<>();
            // Store all record identifiers.
            records.forEach(record -> inputRecords.put(record.getId(), record));
        }

        @Override
        public void handleMatch(final Record record,
                                final ProcessContext context,
                                final MatchingRule matchingRule,
                                MatchQuery.RecordTypeUpdatePolicy recordTypeUpdatePolicy) {
            // Forward processing of tagging to either only first or concat matchHandler.
            this.matchHandler.handleMatch(record, context, matchingRule, recordTypeUpdatePolicy);
            // Remove the processed record from the records to append in case of non-match.
            inputRecords.remove(record.getId());
        }

        @Override
        public Collection<Record> outputRecords() {
            // Get output records from matchHandler...
            Collection<Record> outRecords = this.matchHandler.outputRecords();
            // ...and add all records that did not match any query.
            if (!inputRecords.isEmpty()) {
                inputRecords.values().forEach(record -> outRecords.add(record));
            }
            return outRecords;
        }
    }
}
