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
package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
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

        elasticsearchClientService.setIdentifier("elasticsearchClient");
        runner.addControllerService(elasticsearchClientService);

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
        Thread.sleep(2000);
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
    public void testBulkPut() throws InitializationException, IOException, InterruptedException {
        final String index = "foo";
        final String type = "type1";
        final String docId = "id1";
        final String nameKey = "name";
        final String nameValue = "fred";
        final String ageKey = "age";
        final int ageValue = 33;

        Map<String, Object> document1 = new HashMap<>();
        document1.put(nameKey, nameValue);
        document1.put(ageKey, ageValue);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);

        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index));

        // Create the index
        elasticsearchClientService.createIndex(2, 1, index);
        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index));

        // Put a document in the bulk processor :
        elasticsearchClientService.bulkPut(index, type, document1, Optional.of(docId));
        // Flush the bulk processor :
        elasticsearchClientService.flushBulkProcessor();
        Thread.sleep(2000);
        try {
            // Refresh the index :
            elasticsearchClientService.refreshIndex(index);
        } catch (Exception e) {
            logger.error("Error while refreshing the index : " + e.toString());
        }

        long documentsNumber = 0;

        try {
            documentsNumber = elasticsearchClientService.countIndex(index);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(1, documentsNumber);

        try {
            elasticsearchClientService.saveSync(index, type, document1);
        } catch (Exception e) {
            logger.error("Error while saving the document in the index : " + e.toString());
        }

        try {
            documentsNumber = elasticsearchClientService.countIndex(index);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(2, documentsNumber);

        long numberOfHits = elasticsearchClientService.searchNumberOfHits(index, type, nameKey, nameValue);

        Assert.assertEquals(2, numberOfHits);

    }

    @Test
    public void testMultiGet() throws InitializationException, IOException, InterruptedException, InvalidMultiGetQueryRecordException {
        final String index1 = "index1";
        final String index2 = "index2";
        final String type1 = "type1";
        final String type2 = "type2";
        final String type3 = "type3";

        Map<String, Object> document1 = new HashMap<>();
        final String docId1 = "id1";
        document1.put("field_beg_1", "field_beg_1_document1_value");
        document1.put("field_beg_2", "field_beg_2_document1_value");
        document1.put("field_beg_3", "field_beg_3_document1_value");
        document1.put("field_fin_1", "field_fin_1_document1_value");
        document1.put("field_fin_2", "field_fin_2_document1_value");

        Map<String, Object> document2 = new HashMap<>();
        final String docId2 = "id2";
        document2.put("field_beg_1", "field_beg_1_document2_value");
        document2.put("field_beg_2", "field_beg_2_document2_value");
        document2.put("field_beg_3", "field_beg_3_document2_value");
        document2.put("field_fin_1", "field_fin_1_document2_value");
        document2.put("field_fin_2", "field_fin_2_document2_value");

        Map<String, Object> document3 = new HashMap<>();
        final String docId3 = "id3";
        document3.put("field_beg_1", "field_beg_1_document3_value");
        document3.put("field_beg_2", "field_beg_2_document3_value");
        // this 3rd field is intentionally removed :
        // document3.put("field_beg_3", "field_beg_3_document3_value");
        document3.put("field_fin_1", "field_fin_1_document3_value");
        document3.put("field_fin_2", "field_fin_2_document3_value");

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);

        // Verify the indexes do not exist
        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index1));
        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index2));

        // Create the indexes
        elasticsearchClientService.createIndex(2, 1, index1);
        elasticsearchClientService.createIndex(2, 1, index2);
        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index1));
        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index2));

        // Put documents in the bulk processor :
        elasticsearchClientService.bulkPut(index1, type1, document1, Optional.of(docId1));
        elasticsearchClientService.bulkPut(index1, type1, document2, Optional.of(docId2));
        elasticsearchClientService.bulkPut(index1, type1, document3, Optional.of(docId3));
        elasticsearchClientService.bulkPut(index2, type1, document1, Optional.of(docId1));
        elasticsearchClientService.bulkPut(index2, type2, document2, Optional.of(docId2));
        elasticsearchClientService.bulkPut(index2, type3, document3, Optional.of(docId3));
        // Flush the bulk processor :
        elasticsearchClientService.flushBulkProcessor();
        Thread.sleep(2000);
        try {
            // Refresh the indexes :
            elasticsearchClientService.refreshIndex(index1);
            elasticsearchClientService.refreshIndex(index2);
        } catch (Exception e) {
            logger.error("Error while refreshing the indexes : " + e.toString());
        }

        long countIndex1 = 0;
        long countIndex2 = 0;
        try {
            countIndex1 = elasticsearchClientService.countIndex(index1);
            countIndex2 = elasticsearchClientService.countIndex(index2);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }
        Assert.assertEquals(3, countIndex1);
        Assert.assertEquals(3, countIndex2);

        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
        ArrayList<String> documentIds = new ArrayList<>();
        ArrayList<String> documentIds_2 = new ArrayList<>();
        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();
        String[] fieldsToInclude = {"field_b*", "field*1"};
        String[] fieldsToExclude = {"field_*2"};

        // Make sure a dummy query returns no result :
        documentIds.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", "", documentIds,new String[]{"dummy"},new String[]{}));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
        Assert.assertEquals(0, multiGetResponseRecords.size()); // number of documents retrieved

        multiGetQueryRecords.clear();
        documentIds.clear();
        multiGetResponseRecords.clear();

        // Test : 1 MultiGetQueryRecord record, with 1 index, 1 type, 1 id, WITHOUT includes, WITHOUT excludes :
        documentIds.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, type1, documentIds));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(1, multiGetResponseRecords.size()); // number of documents retrieved
        Assert.assertEquals(index1, multiGetResponseRecords.get(0).getIndexName());
        Assert.assertEquals(type1, multiGetResponseRecords.get(0).getTypeName());
        Assert.assertEquals(docId1, multiGetResponseRecords.get(0).getDocumentId());
        Assert.assertEquals(5, multiGetResponseRecords.get(0).getRetrievedFields().size()); // number of fields retrieved for the document
        multiGetResponseRecords.get(0).getRetrievedFields().forEach((k,v) -> document1.get(k).equals(v.toString()));

        multiGetQueryRecords.clear();
        documentIds.clear();
        multiGetResponseRecords.clear();

        // Test : 1 MultiGetQueryRecord record, with 1 index, 0 type, 3 ids, WITH include, WITH exclude :
        documentIds.add(docId1);
        documentIds.add(docId2);
        documentIds.add(docId3);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, null, documentIds, fieldsToInclude, fieldsToExclude));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(3, multiGetResponseRecords.size()); // verify that 3 documents has been retrieved
        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals(index1, responseRecord.getIndexName())); // verify that all retrieved are in index1
        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals(type1, responseRecord.getTypeName())); // verify that the type of all retrieved documents is type1
        multiGetResponseRecords.forEach(responseRecord -> {
                if (responseRecord.getDocumentId() == docId1) {
                    Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document1, verify that 3 fields has been retrieved
                    // verify that the 3 retrieved fields are the correct ones :
                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_beg_1"));
                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_beg_3"));
                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_fin_1"));
                    // verify that the values of the 3 retrieved fields are the correct ones :
                    Assert.assertEquals("field_beg_1_document1_value", responseRecord.getRetrievedFields().get("field_beg_1").toString());
                    Assert.assertEquals("field_beg_3_document1_value", responseRecord.getRetrievedFields().get("field_beg_3").toString());
                    Assert.assertEquals("field_fin_1_document1_value", responseRecord.getRetrievedFields().get("field_fin_1").toString());
                }
                if (responseRecord.getDocumentId() == docId2)
                    Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document2, verify that 3 fields has been retrieved
                if (responseRecord.getDocumentId() == docId3)
                    Assert.assertEquals(2, responseRecord.getRetrievedFields().size()); // for document3, verify that 2 fields has been retrieved
            });

        multiGetQueryRecords.clear();
        documentIds.clear();
        multiGetResponseRecords.clear();

        // Test : 2 MultiGetQueryRecord records :
        //    - 1st : 1 index (index1), 1 type, 2 ids, WITH include, WITH exclude    --> expecting : 2 docs retrieved (from index1), 3 fields each (except doc3 : 2 fields)
        //    - 2nd : 1 index (index2), 0 type, 3 ids, WITH include, WITHOUT exclude --> expecting : 3 docs retrieved (from index2), 4 fields each (except doc3 : 3 fields)
        documentIds.add(docId1);
        documentIds.add(docId2);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, type1, documentIds, fieldsToInclude, fieldsToExclude));
        documentIds_2.add(docId1);
        documentIds_2.add(docId1);
        documentIds_2.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index2, null , documentIds_2, fieldsToInclude, null));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(5, multiGetResponseRecords.size()); // verify that 5 documents has been retrieved
        multiGetResponseRecords.forEach(responseRecord -> {
            if (responseRecord.getIndexName() == index1 && !responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for documents from index1 (except doc3), verify that 3 fields has been retrieved
            if (responseRecord.getIndexName() == index1 && responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(2, responseRecord.getRetrievedFields().size()); // for document3 from index1, verify that 2 fields has been retrieved
            if (responseRecord.getDocumentId() == index2 && !responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(4, responseRecord.getRetrievedFields().size()); // for documents from index2 (except doc3), verify that 4 fields has been retrieved
            if (responseRecord.getDocumentId() == index2 && responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document3 from index2, verify that 3 fields has been retrieved
        });

    }

    @Test
    public void testMultiGetInvalidRecords() throws InitializationException, IOException, InterruptedException, InvalidMultiGetQueryRecordException {

        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();

        String errorMessage ="";

        // Validate null index behaviour :
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord(null, null, null, null, null));
        }  catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
            Assert.assertEquals(errorMessage,"The index name cannot be null");

        // Validate empty index behaviour :
        try {
        multiGetQueryRecords.add(new MultiGetQueryRecord("", null, null, null, null));
        }  catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertEquals(errorMessage,"The index name cannot be empty");

        // Validate null documentIds behaviour :
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", null, null, null, null));
        }  catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertEquals(errorMessage,"The list of document ids cannot be null");

        // Make sure no invalid MultiGetQueryRecord has been added to multiGetQueryRecords list :
        Assert.assertEquals(0, multiGetQueryRecords.size());
    }
}
