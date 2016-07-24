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
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.utils.elasticsearch.ElasticsearchEventConverter;
import com.hurence.logisland.validators.StandardValidators;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
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

        return Collections.unmodifiableList(descriptors);
    }

   /* @Override
    public void init(ProcessContext context) {
        super.setup(context);
    }
*/

    @Override
    public Collection<Event> process(ProcessContext context, Collection<Event> events) {
        super.setup(context);
        long start = System.currentTimeMillis();
        logger.info("start indexing events");
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        long numItemProcessed = 0L;

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
                .setConcurrentRequests(4)
                .build();

        final String index = context.getProperty(INDEX).getValue();
        final String docType = context.getProperty(TYPE).getValue();

        for (Event event : events) {


            numItemProcessed += 1;

            // Setting ES document id to document's id itself if any (to prevent duplications in ES)
            String idString = UUID.randomUUID().toString();
            if (!Objects.equals(event.getId(), "none")) {
                idString = event.getId();
            }

            String document = ElasticsearchEventConverter.convert(event);
            IndexRequestBuilder result = esClient.get().prepareIndex(index, docType, idString).setSource(document).setOpType(IndexRequest.OpType.CREATE);
            bulkProcessor.add(result.request());
        }

        logger.debug("waiting for remaining items to be flushed");
        try {
            bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
           logger.error(e.getMessage());
        }
        logger.debug("idexed $numItemProcessed records on this partition to es in ${System.currentTimeMillis() - start}");

        logger.info("shutting down es client");
        // on shutdown
        esClient.get().close();
        return Collections.emptyList();
    }


   // @Override
    public Collection<Event> process2(ProcessContext context, Collection<Event> events) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());


        if (events.isEmpty()) {
            return Collections.emptyList();
        }


        // Keep track of the list of flow files that need to be transferred. As they are transferred, remove them from the list.
        List<Event> eventsToTransfer = new LinkedList<>(events);
        try {
            final BulkRequestBuilder bulk = esClient.get().prepareBulk();
            if (authToken != null) {
                bulk.putHeader("Authorization", authToken);
            }
            final String index = context.getProperty(INDEX).getValue();
            final String docType = context.getProperty(TYPE).getValue();

            for (Event file : events) {

                int sentEvents = 0;


                final String id = file.getId();
                if (id == null) {
                    logger.error("No value in identifier attribute {} for {}, transferring to failure", new Object[]{id, file});
                } else {

                    String json = ElasticsearchEventConverter.convert(file);
                    bulk.add(esClient.get().prepareIndex(index, docType, id)
                            .setSource(json.getBytes(charset)));


                }
            }

            final BulkResponse response = bulk.execute().actionGet();
            if (response.hasFailures()) {
                for (final BulkItemResponse item : response.getItems()) {
                    if (item.isFailed()) {
                        logger.error("Failed to insert {} into Elasticsearch due to {}, transferring to failure",
                                item.getFailure().getMessage());

                    }

                }
            }


        } catch (NoNodeAvailableException
                | ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException
                | NodeClosedException exceptionToRetry) {

            // Authorization errors and other problems are often returned as NoNodeAvailableExceptions without a
            // traceable cause. However the cause seems to be logged, just not available to this caught exception.
            // Since the error message will show up as a bulletin, we make specific mention to check the logs for
            // more details.
            logger.error("Failed to insert into Elasticsearch due to {}. More detailed information may be available in " +
                            "the NiFi logs.",
                    new Object[]{exceptionToRetry.getLocalizedMessage()}, exceptionToRetry);
            ;

        } catch (Exception exceptionToFail) {
            logger.error("Failed to insert into Elasticsearch due to {}, transferring to failure",
                    new Object[]{exceptionToFail.getLocalizedMessage()}, exceptionToFail);


        } finally {
            return Collections.emptyList();
        }
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
