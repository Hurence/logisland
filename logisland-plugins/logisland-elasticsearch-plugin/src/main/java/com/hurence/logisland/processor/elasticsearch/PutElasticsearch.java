/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventField;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.utils.elasticsearch.ElasticsearchEventConverter;
import com.hurence.logisland.validators.StandardValidators;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class PutElasticsearch extends AbstractElasticsearchProcessor {

    private static Logger logger = LoggerFactory.getLogger(PutElasticsearch.class);


    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch.size")
            .description("The preferred number of FlowFiles to put to the database in a single transaction")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor TIMEBASED_INDEX = new PropertyDescriptor.Builder()
            .name("timebased.index")
            .description("can be none, today or yesterday")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("yesterday")
            .build();


    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index.field")
            .description("can be none, today or yesterday")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("es_index")
            .build();


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_SHIELD_LOCATION);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);
        descriptors.add(BATCH_SIZE);
        descriptors.add(TIMEBASED_INDEX);
        descriptors.add(ES_INDEX_FIELD);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public Collection<Event> process(ProcessContext context, Collection<Event> events) {
        super.setup(context);
        long start = System.currentTimeMillis();

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        logger.info("start indexing {} events by bulk of {}", events.size(), batchSize);
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");


        long numItemProcessed = 0L;


        /**
         * create the bulk processor
         */
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                esClient.get(),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long l, BulkRequest bulkRequest) {

                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        logger.info(bulkResponse.buildFailureMessage());
                        logger.info("done bulk request in {} ms with failure = {}", bulkResponse.getTookInMillis(), bulkResponse.hasFailures());
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                        logger.error("something went wrong while bulk loading events to es : {}", throwable.getMessage());
                    }

                })
                .setBulkActions(batchSize)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(10)
                .build();

        /**
         * compute global index from Processor settings
         */
        String globalIndex = context.getProperty(INDEX).getValue();
        final String globalType = context.getProperty(TYPE).getValue();
        final String timebased = context.getProperty(TIMEBASED_INDEX).getValue().toLowerCase();
        switch (timebased) {
            case "today":
                globalIndex += "." + sdf.format(new Date());
                break;
            case "yesterday":
                DateTime dt = new DateTime(new Date()).minusDays(1);
                globalIndex += "." + sdf.format(dt.toDate());
                break;
            default:
                break;
        }
        final String esIndexField = context.getProperty(ES_INDEX_FIELD).getValue();


        /**
         * loop over events to add them to bulk
         */
        for (Event event : events) {
            numItemProcessed += 1;

            // Setting ES document id to document's id itself if any (to prevent duplications in ES)
            String docId = UUID.randomUUID().toString();
            if (!Objects.equals(event.getId(), "none")) {
                docId = event.getId();
            }


            // compute es index from event if any
            String docIndex = globalIndex;
            final EventField eventIndex = event.get(esIndexField);
            if (eventIndex != null) {

                EventField eventTime = event.get("event_time");
                if (eventTime != null) {

                    docIndex = eventIndex.getValue().toString();
                    try{
                        long eventTimestamp = (long) eventTime.getValue();
                        switch (timebased) {
                            case "today":
                                docIndex += "." + sdf.format(new Date(eventTimestamp));
                                break;
                            case "yesterday":
                                DateTime dt = new DateTime(eventTimestamp).minusDays(1);
                                docIndex += "." + sdf.format(dt.toDate());
                                break;
                            default:
                                break;
                        }
                    }catch (Exception e){
                        logger.info("unable to convert event_time {}", e.getMessage());
                    }


                } else
                    docIndex = eventIndex.getValue().toString();
            }

            // compute es type from event if any
            String docType = globalType;
            if (!Objects.equals(event.getType(), "none")) {
                docType = event.getType();
            }

            // dump event to a JSON format
            String document = ElasticsearchEventConverter.convert(event);

            // add it to the bulk
            IndexRequestBuilder result = esClient.get()
                    .prepareIndex(docIndex, docType, docId)
                    .setSource(document)
                    .setOpType(IndexRequest.OpType.CREATE);
            bulkProcessor.add(result.request());
        }
        logger.info("sent {} events to elasticsearch", numItemProcessed);
        bulkProcessor.flush();


        /**
         * fluch remaining items
         */
        logger.info("waiting for remaining items to be flushed");
        try {
            bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
        logger.info("done in {}", System.currentTimeMillis() - start);


        /**
         * closing
         */
        logger.info("shutting down es client");
        esClient.get().close();
        return Collections.emptyList();
    }


    /**
     * Dispose of ElasticSearch client
     */
    public void closeClient() {
        super.closeClient();
    }


    @Override
    public String getIdentifier() {
        return null;
    }
}
