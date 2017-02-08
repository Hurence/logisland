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
package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.elasticsearch.ElasticsearchRecordConverter;
import com.hurence.logisland.validator.StandardValidators;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.*;
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


@Tags({"record", "elasticsearch", "sink", "record"})
@CapabilityDescription("This is a processor that puts records to ES")
public class PutElasticsearch extends AbstractElasticsearchProcessor {

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
            .description("The preferred number of Records to setField to the database in a single transaction")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .name("bulk.size")
            .description("bulk size in MB")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("flush.interval")
            .description("flush interval in sec")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor CONCURRENT_REQUESTS = new PropertyDescriptor.Builder()
            .name("concurrent.requests")
            .description("setConcurrentRequests")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    public static final PropertyDescriptor BULK_RETRY_NUMBER = new PropertyDescriptor.Builder()
            .name("num.retry")
            .description("number of time we should try to inject a bulk into es")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor BULK_THROTTLING_DELAY = new PropertyDescriptor.Builder()
            .name("throttling.delay")
            .description("number of time we should wait between each retry (in milliseconds)")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("500")
            .build();

    public static final AllowableValue NO_BACKOFF_POLICY = new AllowableValue("noBackoff", "No retry policy",
            "when a request fail there won't be any retry.");

    public static final AllowableValue CONSTANT_BACKOFF_POLICY = new AllowableValue("constantBackoff", "wait a fixed amount of time between retries",
            "wait a fixed amount of time between retries, using user put retry number and throttling delay");

    public static final AllowableValue EXPONENTIAL_BACKOFF_POLICY = new AllowableValue("exponentialBackoff", "custom exponential policy",
            "time waited between retries grow exponentially, using user put retry number and throttling delay");

    public static final AllowableValue DEFAULT_EXPONENTIAL_BACKOFF_POLICY = new AllowableValue("defaultExponentialBackoff", "es default exponential policy",
            "time waited between retries grow exponentially, using es default parameters");

    public static final PropertyDescriptor BULK_BACK_OFF_POLICY = new PropertyDescriptor.Builder()
            .name("backoff.policy")
            .description("strategy for retrying to execute requests in bulkRequest")
            .required(true)
            .allowableValues(NO_BACKOFF_POLICY, CONSTANT_BACKOFF_POLICY, EXPONENTIAL_BACKOFF_POLICY, DEFAULT_EXPONENTIAL_BACKOFF_POLICY)
            .defaultValue(DEFAULT_EXPONENTIAL_BACKOFF_POLICY.getValue())
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
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type.field")
            .description("the name of the event field containing es doc type => will override type value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
        descriptors.add(BULK_RETRY_NUMBER);
        descriptors.add(BULK_THROTTLING_DELAY);
        descriptors.add(BULK_BACK_OFF_POLICY);
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
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        super.setup(context);

        final Collection<Record> failedRecords = new ArrayList<>();
        if (records.size() != 0) {

            /**
             * set up BackoffPolicy
             */
            BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
            if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(DEFAULT_EXPONENTIAL_BACKOFF_POLICY.getValue())){
                backoffPolicy = BackoffPolicy.exponentialBackoff();
            } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(EXPONENTIAL_BACKOFF_POLICY.getValue())) {
                backoffPolicy = BackoffPolicy.exponentialBackoff(
                        TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                        context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
                );
            } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(CONSTANT_BACKOFF_POLICY.getValue())) {
                backoffPolicy = BackoffPolicy.constantBackoff(
                        TimeValue.timeValueMillis(context.getPropertyValue(BULK_THROTTLING_DELAY).asLong()),
                        context.getPropertyValue(BULK_RETRY_NUMBER).asInteger()
                );
            } else if (context.getPropertyValue(BULK_BACK_OFF_POLICY).getRawValue().equals(NO_BACKOFF_POLICY.getValue())) {
                backoffPolicy = BackoffPolicy.noBackoff();
            }

            final Map<String/*id*/, String/*errors*/> failedIds = new HashMap<>();
            /**
             * create the bulk processor
             */
            BulkProcessor bulkProcessor = BulkProcessor.builder(
                    esClient.get(),
                    new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long l, BulkRequest bulkRequest) {
                            logger.debug("Going to execute bulk [id:{}] composed of {} actions", l, bulkRequest.numberOfActions());
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                            logger.debug("Executed bulk [id:{}] composed of {} actions", l, bulkRequest.numberOfActions());

                            if (bulkResponse.hasFailures()) {
                                logger.warn("There was failures while executing bulk [id:{}]," +
                                        " done bulk request in {} ms with failure = {}",
                                        l, bulkResponse.getTookInMillis(), bulkResponse.buildFailureMessage());
                                for (BulkItemResponse item : bulkResponse.getItems()) {
                                    if (item.isFailed()) {
                                        failedIds.put(item.getId(), item.getFailureMessage());
//                                        List<String> errors = null;
//                                        if (failedIds.containsKey(item.getId())) {
//                                            errors = failedIds.get(item.getId());
//                                        } else {
//                                            errors = new LinkedList<String>();
//                                            failedIds.put(item.getId(), errors);
//                                        }
//                                        errors.add(item.getFailureMessage());
                                    }
                                }

                            }
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                            logger.error("something went wrong while bulk loading events to es : {}", throwable.getMessage());
                            failedRecords.addAll(records);
                        }

                    })
                    .setBulkActions(context.getPropertyValue(BATCH_SIZE).asInteger())
                    .setBulkSize(new ByteSizeValue(context.getPropertyValue(BULK_SIZE).asInteger(), ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(context.getPropertyValue(FLUSH_INTERVAL).asInteger()))
                    .setConcurrentRequests(context.getPropertyValue(CONCURRENT_REQUESTS).asInteger())
                    .setBackoffPolicy(backoffPolicy)
                    .build();


            /**
             * compute global index from Processor settings
             */
            String defaultIndex = context.getPropertyValue(DEFAULT_INDEX).asString();
            String defaultType = context.getPropertyValue(DEFAULT_TYPE).asString();
            if (context.getPropertyValue(TIMEBASED_INDEX).isSet()) {
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
                if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(TODAY_DATE_SUFFIX.getValue())) {
                    defaultIndex += "." + sdf.format(new Date());
                } else if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(YESTERDAY_DATE_SUFFIX.getValue())) {
                    DateTime dt = new DateTime(new Date()).minusDays(1);
                    defaultIndex += "." + sdf.format(dt.toDate());
                }
            }


            /**
             * loop over events to add them to bulk
             */
            for (Record record : records) {

                // compute es index from event if any
                String docIndex = defaultIndex;
                if (context.getPropertyValue(ES_INDEX_FIELD).isSet()) {
                    Field eventIndexField = record.getField(context.getPropertyValue(ES_INDEX_FIELD).asString());
                    if (eventIndexField != null && eventIndexField.getRawValue() != null) {
                        docIndex = eventIndexField.getRawValue().toString();
                    }
                }

                // compute es type from event if any
                String docType = defaultType;
                if (context.getPropertyValue(ES_TYPE_FIELD).isSet()) {
                    Field eventTypeField = record.getField(context.getPropertyValue(ES_TYPE_FIELD).asString());
                    if (eventTypeField != null && eventTypeField.getRawValue() != null) {
                        docType = eventTypeField.getRawValue().toString();
                    }
                }

                // dump event to a JSON format
                String document = ElasticsearchRecordConverter.convert(record);

                // add it to the bulk
                IndexRequestBuilder result = esClient.get()
                        .prepareIndex(docIndex, docType)
                        .setId(record.getId())
                        .setSource(document)
                        .setOpType(IndexRequest.OpType.CREATE);
                bulkProcessor.add(result.request());
            }
            bulkProcessor.flush();

            /**
             * fluch remaining items
             */
            try {
                if (!bulkProcessor.awaitClose(10, TimeUnit.SECONDS)) {
                    logger.error("some request could not be send to es because of time out");
                } else {
                    logger.info("all requests have been submitted to es");
                }
                /**
                 * loop over events to add failed ones
                 */
                if (!failedIds.isEmpty()) {
                    for (Record record : records) {
                        if (failedIds.keySet().contains(record.getId())) {
                            failedRecords.add(record);
                            record.addError(ProcessError.REGEX_MATCHING_ERROR.getName(), failedIds.get(record.getId()));
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }


            esClient.get().close();
        }

        return failedRecords;
    }


    /**
     * Dispose of ElasticSearch client
     */
    public void closeClient() {
        super.closeClient();
    }


}
