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
package com.hurence.logisland.service.elasticsearchasaservice;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.processor.elasticsearchasaservice.ElasticsearchClientService;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TestElasticsearch_2_3_3_ClientService {

    private static final String MAPPING1 = "{'properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'val':{'type':'integer'}}}";
    private static final String MAPPING2 = "{'properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'val':{'type': 'string', 'index': 'not_analyzed'}}}";
    private static final String MAPPING3 =
            "{'dynamic':'strict','properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'xyz':{'type': 'string', 'index': 'not_analyzed'}}}";

    private static Logger logger = LoggerFactory.getLogger(TestElasticsearch_2_3_3_ClientService.class);

    @Rule
    public final ESRule esRule = new ESRule();


    private class MockElasticsearchClientService extends Elasticsearch_2_3_3_ClientService {

        @Override
        protected void createElasticsearchClient(ControllerServiceInitializationContext context) throws ProcessException {
            if (esClient != null) {
                return;
            }
            esClient = esRule.getClient();
        }

        @Override
        protected void createBulkProcessor(ControllerServiceInitializationContext context)
        {
            if (bulkProcessor != null) {
                return;
            }

            /**
             * create the bulk processor
             */
            bulkProcessor = BulkProcessor.builder(
                    esClient,
                    new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long l, BulkRequest bulkRequest) {
                            getLogger().debug("Going to execute bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                            getLogger().debug("Executed bulk [id:{}] composed of {} actions", new Object[]{l, bulkRequest.numberOfActions()});
                            if (bulkResponse.hasFailures()) {
                                getLogger().warn("There was failures while executing bulk [id:{}]," +
                                                " done bulk request in {} ms with failure = {}",
                                        new Object[]{l, bulkResponse.getTookInMillis(), bulkResponse.buildFailureMessage()});
                                for (BulkItemResponse item : bulkResponse.getItems()) {
                                    if (item.isFailed()) {
                                        errors.put(item.getId(), item.getFailureMessage());
                                    }
                                }
                            }
                        }

                        @Override
                        public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                            getLogger().error("something went wrong while bulk loading events to es : {}", new Object[]{throwable.getMessage()});
                        }

                    })
                    .setBulkActions(1000)
                    .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(1))
                    .setConcurrentRequests(2)
                    //.setBackoffPolicy(getBackOffPolicy(context))
                    .build();
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
        }

    }

    private ElasticsearchClientService configureElasticsearchClientService(final TestRunner runner) throws InitializationException
    {
        final MockElasticsearchClientService elasticsearchClientService = new MockElasticsearchClientService();

        runner.addControllerService("elasticsearchClient", elasticsearchClientService);

        runner.enableControllerService(elasticsearchClientService);
        runner.setProperty(TestProcessor.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");
        runner.assertValid(elasticsearchClientService);

        // TODO : is this necessary ?
        final ElasticsearchClientService service = runner.getProcessContext().getPropertyValue(TestProcessor.ELASTICSEARCH_CLIENT_SERVICE).asControllerService(ElasticsearchClientService.class);
        return service;
    }

    @Test
    public void testBasics() throws Exception {

        Map<String, Object> document1 = new HashMap<>();
        document1.put("name", "fred");
        document1.put("val", 33);

        boolean result;

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);


        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsIndex("foo"));

        // Define the index
        elasticsearchClientService.createIndex(2, 1, "foo");
        Assert.assertEquals(true, elasticsearchClientService.existsIndex("foo"));

        // Define another index
        elasticsearchClientService.createIndex(2, 1, "bar");
        Assert.assertEquals(true, elasticsearchClientService.existsIndex("foo"));

        // Add a mapping to foo
        result = elasticsearchClientService.putMapping("foo", "type1", MAPPING1.replace('\'', '"'));
//        Assert.assertEquals(true, result);

        // Add the same mapping again
        result = elasticsearchClientService.putMapping("foo", "type1", MAPPING1.replace('\'', '"'));
        //       Assert.assertEquals(true, result);

        // Add the same mapping again under a different doctype
        result = elasticsearchClientService.putMapping("foo", "type2", MAPPING1.replace('\'', '"'));
        //       Assert.assertEquals(true, result);

        // Update a mapping with an incompatible mapping -- should fail
        //result = elasticsearchClientService.putMapping("foo", "type2", MAPPING2.replace('\'', '"'));
        //Assert.assertEquals(false, result);

        // create alias
        elasticsearchClientService.createAlias("foo", "aliasFoo");
        Assert.assertEquals(true, elasticsearchClientService.existsIndex("aliasFoo"));

        // Insert a record into foo and count foo
        Assert.assertEquals(0, elasticsearchClientService.countIndex("foo"));
        elasticsearchClientService.saveSync("foo", "type1", document1);
        Assert.assertEquals(1, elasticsearchClientService.countIndex("foo"));

        // copy index foo to bar - should work
        Assert.assertEquals(0, elasticsearchClientService.countIndex("bar"));
        elasticsearchClientService.copyIndex(TimeValue.timeValueMinutes(2).toString(), "foo", "bar");
        elasticsearchClientService.flushBulkProcessor();
        Thread.sleep(500);
        elasticsearchClientService.refreshIndex("bar");
        Assert.assertEquals(1, elasticsearchClientService.countIndex("bar"));

        // Define incompatible mappings for the same doctype in two different indexes, then try to copy - should fail
        // as a document registered with doctype=type1 in index foo cannot be written as doctype=type1 in index baz.
        //
        // Note: MAPPING2 cannot be added to index foo or bar at all, even under a different doctype, as ES (lucene)
        // does not allow two types for the same field-name in different mappings of the same index. However if
        // MAPPING2 is added to index baz, then the copyIndex succeeds - because by default ES automatically converts
        // integers into strings when necessary. Interestingly, this means MAPPING1 and MAPPING2 are not compatible
        // at the "put mapping" level, but are compatible at the "reindex" level..
        //
        // The document (doc1) of type "type1" already in index "foo" cannot be inserted into index "baz" as type1
        // because that means applying its source to MAPPING3 - but MAPPING3 is strict and does not define property
        // "val", so the insert fails.
        elasticsearchClientService.createIndex(2, 1, "baz");
        elasticsearchClientService.putMapping("baz", "type1", MAPPING3.replace('\'', '"'));

      /*  try {
            elasticsearchClientService.copyIndex(TimeValue.timeValueMinutes(2), "foo", "baz");
            Assert.fail("Exception not thrown when expected");
        } catch(IOException e) {
            Assert.assertTrue(e.getMessage().contains("Reindex failed"));
        }*/
        elasticsearchClientService.refreshIndex("baz");
        Assert.assertEquals(0, elasticsearchClientService.countIndex("baz"));

        // Drop index foo
        elasticsearchClientService.dropIndex("foo");
        Assert.assertEquals(false, elasticsearchClientService.existsIndex("foo"));
        Assert.assertEquals(false, elasticsearchClientService.existsIndex("aliasFoo")); // alias for foo disappears too
        Assert.assertEquals(true, elasticsearchClientService.existsIndex("bar"));
    }

    @Test
    public void testSinglePut() throws InitializationException, IOException, InterruptedException {
        final String docIndex = "foo";
        final String docType = "type1";
        final String docId = "id1";
        final String nameKey = "name";
        final String nameValue = "fred";
        final String ageKey = "age";
        final int ageValue = 33;

        Map<String, Object> document1 = new HashMap<>();
        document1.put(nameKey, nameValue);
        document1.put(ageKey, ageValue);

        String test = (String) document1.get(nameKey);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);

        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsIndex(docIndex));

        // Create the index
        elasticsearchClientService.createIndex(2, 1, docIndex);
        Assert.assertEquals(true, elasticsearchClientService.existsIndex(docIndex));

        // Put a document in the bulk processor :
        elasticsearchClientService.bulkPut(docIndex, docType, document1, Optional.of(docId));
        // Flush the bulk processor :
        elasticsearchClientService.flushBulkProcessor();
        Thread.sleep(500);
        try {
            // Refresh the index :
            elasticsearchClientService.refreshIndex(docIndex);
        } catch (Exception e) {
            logger.error("Error while refreshing the index : " + e.toString());
        }

        long documentsNumber = 0;

        try {
            documentsNumber = elasticsearchClientService.countIndex(docIndex);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(1, documentsNumber);

        try {
            elasticsearchClientService.saveSync(docIndex, docType, document1);
        } catch (Exception e) {
            logger.error("Error while saving the document in the index : " + e.toString());
        }

        try {
            documentsNumber = elasticsearchClientService.countIndex(docIndex);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(2, documentsNumber);

        long numberOfHits = elasticsearchClientService.searchNumberOfHits(docIndex, docType, nameKey, nameValue);

        Assert.assertEquals(2, numberOfHits);

    }

}
