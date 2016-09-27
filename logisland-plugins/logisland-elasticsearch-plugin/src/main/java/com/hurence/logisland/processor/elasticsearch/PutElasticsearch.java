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


import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.utils.elasticsearch.ElasticsearchEventConverter;
import com.hurence.logisland.validator.StandardPropertyValidators;
import org.elasticsearch.action.bulk.BackoffPolicy;
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

/**
 * a processor class that puts events to ES
 */
public class PutElasticsearch extends AbstractElasticsearchProcessor {

    static final long serialVersionUID = -5661820708591436282L;

    private static Logger logger = LoggerFactory.getLogger(PutElasticsearch.class);


    public static final PropertyDescriptor DEFAULT_INDEX = new PropertyDescriptor.Builder()
            .name("default.index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DEFAULT_TYPE = new PropertyDescriptor.Builder()
            .name("default.type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch.size")
            .description("The preferred number of FlowFiles to setField to the database in a single transaction")
            .required(false)
            .addValidator(StandardPropertyValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .name("bulk.size")
            .description("bulk size in MB")
            .required(false)
            .addValidator(StandardPropertyValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("flush.interval")
            .description("flush interval in sec")
            .required(false)
            .addValidator(StandardPropertyValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor CONCURRENT_REQUESTS = new PropertyDescriptor.Builder()
            .name("concurrent.requests")
            .description("setConcurrentRequests")
            .required(false)
            .addValidator(StandardPropertyValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();


    public static final AllowableValue NO_DATE_SUFFIX = new AllowableValue("no", "No date",
            "no date added to default index");

    public static final AllowableValue TODAY_DATE_SUFFIX = new AllowableValue("today", "Today's date",
            "today's date added to default index");

    public static final AllowableValue YESTERDAY_DATE_SUFFIX = new AllowableValue("yesterday", "yesterday's date",
            "yesterday's date added to default index");

    public static final PropertyDescriptor TIMEBASED_INDEX = new PropertyDescriptor.Builder()
            .name("timebased.index")
            .description("do we add a date suffix")
            .required(true)
            .allowableValues(NO_DATE_SUFFIX, TODAY_DATE_SUFFIX, YESTERDAY_DATE_SUFFIX)
            .defaultValue(NO_DATE_SUFFIX.getValue())
            .build();


    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index.field")
            .description("the name of the event field containing es index type => will override index value if set")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type.field")
            .description("the name of the event field containing es doc type => will override type value if set")
            .required(false)
            .addValidator(StandardPropertyValidators.NON_EMPTY_VALIDATOR)
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
        descriptors.add(DEFAULT_INDEX);
        descriptors.add(DEFAULT_TYPE);
        descriptors.add(CHARSET);
        descriptors.add(BATCH_SIZE);
        descriptors.add(BULK_SIZE);
        descriptors.add(CONCURRENT_REQUESTS);
        descriptors.add(FLUSH_INTERVAL);
        descriptors.add(TIMEBASED_INDEX);
        descriptors.add(ES_INDEX_FIELD);
        descriptors.add(ES_TYPE_FIELD);

        return Collections.unmodifiableList(descriptors);
    }

    /**
     * process events
     *
     * @param context
     * @param records
     * @return
     */
    @Override
    public Collection<Record> process(ComponentContext context, Collection<Record> records) {
        super.setup(context);

        if (records.size() != 0) {

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
                            if(bulkResponse.hasFailures()){
                                logger.info(bulkResponse.buildFailureMessage());
                                logger.info("done bulk request in {} ms with failure = {}", bulkResponse.getTookInMillis(), bulkResponse.hasFailures());
                            }

                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                            logger.error("something went wrong while bulk loading events to es : {}", throwable.getMessage());
                        }

                    })
                    .setBulkActions(context.getProperty(BATCH_SIZE).asInteger())
                    .setBulkSize(new ByteSizeValue(context.getProperty(BULK_SIZE).asInteger(), ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(context.getProperty(FLUSH_INTERVAL).asInteger()))
                    .setConcurrentRequests(context.getProperty(CONCURRENT_REQUESTS).asInteger())
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                    .build();


            /**
             * compute global index from Processor settings
             */
            String defaultIndex = context.getProperty(DEFAULT_INDEX).asString();
            String defaultType = context.getProperty(DEFAULT_TYPE).asString();
            if (context.getProperty(TIMEBASED_INDEX).isSet()) {
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
                if (context.getProperty(TIMEBASED_INDEX).getRawValue().equals(TODAY_DATE_SUFFIX.getValue())) {
                    defaultIndex += "." + sdf.format(new Date());
                } else if (context.getProperty(TIMEBASED_INDEX).getRawValue().equals(YESTERDAY_DATE_SUFFIX.getValue())) {
                    DateTime dt = new DateTime(new Date()).minusDays(1);
                    defaultIndex += "." + sdf.format(dt.toDate());
                }
            }


            /**
             * loop over events to add them to bulk
             */
            for (Record record : records) {
                numItemProcessed += 1;

                // compute es index from event if any
                String docIndex = defaultIndex;
                if (context.getProperty(ES_INDEX_FIELD).isSet()) {
                    Field eventIndexField = record.getField(context.getProperty(ES_INDEX_FIELD).asString());
                    if (eventIndexField != null && eventIndexField.getRawValue() != null) {
                        docIndex = eventIndexField.getRawValue().toString();
                    }
                }

                // compute es type from event if any
                String docType = defaultType;
                if (context.getProperty(ES_TYPE_FIELD).isSet()) {
                    Field eventTypeField = record.getField(context.getProperty(ES_TYPE_FIELD).asString());
                    if (eventTypeField != null && eventTypeField.getRawValue() != null) {
                        docType = eventTypeField.getRawValue().toString();
                    }
                }

                // dump event to a JSON format
                String document = ElasticsearchEventConverter.convert(record);

                // add it to the bulk
                IndexRequestBuilder result = esClient.get()
                        .prepareIndex(docIndex, docType)
                        .setSource(document)
                        .setOpType(IndexRequest.OpType.CREATE);
                bulkProcessor.add(result.request());
            }
            bulkProcessor.flush();


            /**
             * fluch remaining items
             */
            try {
                bulkProcessor.awaitClose(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }


            esClient.get().close();
        }

        return Collections.emptyList();
    }


    /**
     * Dispose of ElasticSearch client
     */
    public void closeClient() {
        super.closeClient();
    }


}
