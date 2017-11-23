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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TestSolr_5_5_5_ClientService {

    private static final String MAPPING1 = "{'properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'val':{'type':'integer'}}}";
    private static final String MAPPING2 = "{'properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'val':{'type': 'string', 'index': 'not_analyzed'}}}";
    private static final String MAPPING3 =
            "{'dynamic':'strict','properties':{'name':{'type': 'string', 'index': 'not_analyzed'},'xyz':{'type': 'string', 'index': 'not_analyzed'}}}";

    private static Logger logger = LoggerFactory.getLogger(TestSolr_5_5_5_ClientService.class);

    @Rule
    public final SolrRule solrRule = new SolrRule();


    private class MockSolrClientService extends Solr_5_5_5_ClientService {

        @Override
        protected void createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
            if (solrClient != null) {
                return;
            }
            solrClient = solrRule.getClient();
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

            List<PropertyDescriptor> props = new ArrayList<>();

            return Collections.unmodifiableList(props);
        }

    }

    private Solr_5_5_5_ClientService configureSolrClientService(final TestRunner runner) throws InitializationException
    {
        final MockSolrClientService solrClientService = new MockSolrClientService();

        runner.addControllerService("solrClient", solrClientService);

        runner.enableControllerService(solrClientService);
        runner.setProperty(TestProcessor.SOLR_CLIENT_SERVICE, "solrClient");
        runner.assertValid(solrClientService);

        // TODO : is this necessary ?
        final Solr_5_5_5_ClientService service = runner.getProcessContext().getPropertyValue(TestProcessor.SOLR_CLIENT_SERVICE).asControllerService(Solr_5_5_5_ClientService.class);
        return service;
    }

    @Test
    public void testBasics() throws Exception {
        Record record1 = new StandardRecord()
                .setId("record1")
                .setStringField("name_s", "fred");

        boolean result;

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        final Solr_5_5_5_ClientService solrClientService = configureSolrClientService(runner);

        solrClientService.dropCollection("foo");
        solrClientService.dropCollection("bar");
        solrClientService.dropCollection("baz");


        // Verify the index does not exist
        Assert.assertEquals(false, solrClientService.existsCollection("foo"));

        // Define the index
        solrClientService.createCollection("foo", 1, 0);
        Assert.assertEquals(true, solrClientService.existsCollection("foo"));

        // Define another index
        solrClientService.createCollection("bar",1, 0);
        Assert.assertEquals(true, solrClientService.existsCollection("bar"));

        List<Map<String, Object>> mapping1 = new ArrayList<>();

        Map<String, Object> nameField = new LinkedHashMap<>();
        nameField.put("name", "name");
        nameField.put("type", "string");
        nameField.put("stored", true);
        mapping1.add(nameField);

        Map<String, Object> valField = new LinkedHashMap<>();
        valField.put("name", "val");
        valField.put("type", "int");
        nameField.put("stored", true);
        mapping1.add(valField);


        // Add a mapping to foo
        result = solrClientService.putMapping("foo", mapping1);
        Assert.assertEquals(true, result);

        // Add the same mapping again
        result = solrClientService.putMapping("bar", mapping1);
        Assert.assertEquals(true, result);

        List<Map<String, Object>> mapping2 = new ArrayList<>();

        Map<String, Object> valStringField = new LinkedHashMap<>();
        valField.put("name", "val");
        valField.put("type", "string");
        mapping2.add(valStringField);


        // Update a mapping with an incompatible mapping -- should fail
 //       result = solrClientService.putMapping("foo", mapping2);
//        Assert.assertEquals(false, result);

        // create alias
        // TODO - Manage Solr Cloud mode
//        solrClientService.createAlias("foo", "aliasFoo");
//        Assert.assertEquals(true, solrClientService.existsCollection("aliasFoo"));
//
//        // Insert a record into foo and count foo
        Assert.assertEquals(0, solrClientService.countCollection("foo"));
        solrClientService.put("foo", record1);
        Assert.assertEquals(1, solrClientService.countCollection("foo"));

        // copy index foo to baz - should work
        Assert.assertEquals(false, solrClientService.existsCollection("baz"));
        solrClientService.createCollection("baz");
        Assert.assertEquals(true, solrClientService.existsCollection("baz"));
        Assert.assertEquals(0, solrClientService.countCollection("baz"));
        solrClientService.copyCollection("0", "foo", "baz");
        Assert.assertEquals(1, solrClientService.countCollection("baz"));

        // Drop index foo
        solrClientService.dropCollection("foo");
        Assert.assertEquals(false, solrClientService.existsCollection("foo"));
        Assert.assertEquals(false, solrClientService.existsCollection("aliasFoo")); // alias for foo disappears too
        Assert.assertEquals(true, solrClientService.existsCollection("baz"));
    }

    @Test
    public void testBulkPut() throws InitializationException, IOException, InterruptedException {
        final String collection = "foo";
        final String docId = "id1";
        final String docId2 = "id2";
        final String nameKey = "name_s";
        final String nameValue = "fred";
        final String ageKey = "age_i";
        final int ageValue = 33;

        Record document1 = new StandardRecord();
        document1.setId(docId);
        document1.setStringField(nameKey, nameValue);
        document1.setField(ageKey, FieldType.INT, ageValue);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor :
        final Solr_5_5_5_ClientService solrClientService = configureSolrClientService(runner);

        solrClientService.dropCollection(collection);

        // Verify the index does not exist
        Assert.assertEquals(false, solrClientService.existsCollection(collection));

        // Create the index
        solrClientService.createCollection(collection);
        Assert.assertEquals(true, solrClientService.existsCollection(collection));

        // Put a document in the bulk processor :
        solrClientService.bulkPut(collection, document1);
        // Flush the bulk processor :
        solrClientService.bulkFlush(collection);

        Assert.assertEquals(1, solrClientService.countCollection(collection));

        document1.setId(docId2);
        solrClientService.put(collection, document1);

        Assert.assertEquals(2, solrClientService.countCollection(collection));
        Assert.assertEquals(2, solrClientService.queryCount(collection, nameKey+":"+nameValue));
    }


    @Test
    public void testBulkPutGeopoint() throws InitializationException, IOException, InterruptedException {
        final String index = "future_factory";
        final String type = "factory";
        final String docId = "modane_factory";
        Record record = new StandardRecord("factory")
                .setId(docId)
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 45.4f);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);

        // create the controller service and link it to the test processor :
//        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);
//
//        // Verify the index does not exist
//        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index));
//
//        // Create the index
//        elasticsearchClientService.createIndex(2, 1, index);
//        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index));
//
//        // Put a document in the bulk processor :
//        String document1 = ElasticsearchRecordConverter.convertToString(record);
//        elasticsearchClientService.bulkPut(index, type, document1, Optional.of(docId));
//        // Flush the bulk processor :
//        elasticsearchClientService.flushBulkProcessor();
//        Thread.sleep(2000);
//        try {
//            // Refresh the index :
//            elasticsearchClientService.refreshIndex(index);
//        } catch (Exception e) {
//            logger.error("Error while refreshing the index : " + e.toString());
//        }
//
//        long documentsNumber = 0;
//
//        try {
//            documentsNumber = elasticsearchClientService.countIndex(index);
//        } catch (Exception e) {
//            logger.error("Error while counting the number of documents in the index : " + e.toString());
//        }
//
//        Assert.assertEquals(1, documentsNumber);
//
//        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
//        ArrayList<String> documentIds = new ArrayList<>();
//        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();
//
//
//        // Make sure a dummy query returns no result :
//        documentIds.add(docId);
//        try {
//            multiGetQueryRecords.add(new MultiGetQueryRecord(index, type,new String[]{"location", "id"},new String[]{}, documentIds));
//        } catch (InvalidMultiGetQueryRecordException e) {
//            e.printStackTrace();
//        }
//        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
//        Assert.assertEquals(1, multiGetResponseRecords.size()); // number of documents retrieved

    }



    @Test
    public void testMultiGet() throws InitializationException, IOException, InterruptedException {
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
//        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchClientService(runner);
//
//        // Verify the indexes do not exist
//        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index1));
//        Assert.assertEquals(false, elasticsearchClientService.existsIndex(index2));
//
//        // Create the indexes
//        elasticsearchClientService.createIndex(2, 1, index1);
//        elasticsearchClientService.createIndex(2, 1, index2);
//        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index1));
//        Assert.assertEquals(true, elasticsearchClientService.existsIndex(index2));
//
//        // Put documents in the bulk processor :
//        elasticsearchClientService.bulkPut(index1, type1, document1, Optional.of(docId1));
//        elasticsearchClientService.bulkPut(index1, type1, document2, Optional.of(docId2));
//        elasticsearchClientService.bulkPut(index1, type1, document3, Optional.of(docId3));
//        elasticsearchClientService.bulkPut(index2, type1, document1, Optional.of(docId1));
//        elasticsearchClientService.bulkPut(index2, type2, document2, Optional.of(docId2));
//        elasticsearchClientService.bulkPut(index2, type3, document3, Optional.of(docId3));
//        // Flush the bulk processor :
//        elasticsearchClientService.flushBulkProcessor();
//        Thread.sleep(2000);
//        try {
//            // Refresh the indexes :
//            elasticsearchClientService.refreshIndex(index1);
//            elasticsearchClientService.refreshIndex(index2);
//        } catch (Exception e) {
//            logger.error("Error while refreshing the indexes : " + e.toString());
//        }
//
//        long countIndex1 = 0;
//        long countIndex2 = 0;
//        try {
//            countIndex1 = elasticsearchClientService.countIndex(index1);
//            countIndex2 = elasticsearchClientService.countIndex(index2);
//        } catch (Exception e) {
//            logger.error("Error while counting the number of documents in the index : " + e.toString());
//        }
//        Assert.assertEquals(3, countIndex1);
//        Assert.assertEquals(3, countIndex2);
//
//        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
//        ArrayList<String> documentIds = new ArrayList<>();
//        ArrayList<String> documentIds_2 = new ArrayList<>();
//        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();
//        String[] fieldsToInclude = {"field_b*", "field*1"};
//        String[] fieldsToExclude = {"field_*2"};
//
//        // Make sure a dummy query returns no result :
//        documentIds.add(docId1);
//        multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", "",new String[]{"dummy"},new String[]{}, documentIds));
//        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
//        Assert.assertEquals(0, multiGetResponseRecords.size()); // number of documents retrieved
//
//        multiGetQueryRecords.clear();
//        documentIds.clear();
//        multiGetResponseRecords.clear();
//
//        // Test : 1 MultiGetQueryRecord record, with 1 index, 1 type, 1 id, WITHOUT includes, WITHOUT excludes :
//        documentIds.add(docId1);
//        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, type1, documentIds));
//        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
//
//        Assert.assertEquals(1, multiGetResponseRecords.size()); // number of documents retrieved
//        Assert.assertEquals(index1, multiGetResponseRecords.get(0).getIndexName());
//        Assert.assertEquals(type1, multiGetResponseRecords.get(0).getTypeName());
//        Assert.assertEquals(docId1, multiGetResponseRecords.get(0).getDocumentId());
//        Assert.assertEquals(5, multiGetResponseRecords.get(0).getRetrievedFields().size()); // number of fields retrieved for the document
//        multiGetResponseRecords.get(0).getRetrievedFields().forEach((k,v) -> document1.get(k).equals(v.toString()));
//
//        multiGetQueryRecords.clear();
//        documentIds.clear();
//        multiGetResponseRecords.clear();
//
//        // Test : 1 MultiGetQueryRecord record, with 1 index, 0 type, 3 ids, WITH include, WITH exclude :
//        documentIds.add(docId1);
//        documentIds.add(docId2);
//        documentIds.add(docId3);
//        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, null, fieldsToInclude, fieldsToExclude, documentIds));
//        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
//
//        Assert.assertEquals(3, multiGetResponseRecords.size()); // verify that 3 documents has been retrieved
//        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals(index1, responseRecord.getIndexName())); // verify that all retrieved are in index1
//        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals(type1, responseRecord.getTypeName())); // verify that the type of all retrieved documents is type1
//        multiGetResponseRecords.forEach(responseRecord -> {
//                if (responseRecord.getDocumentId() == docId1) {
//                    Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document1, verify that 3 fields has been retrieved
//                    // verify that the 3 retrieved fields are the correct ones :
//                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_beg_1"));
//                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_beg_3"));
//                    Assert.assertEquals(true, responseRecord.getRetrievedFields().containsKey("field_fin_1"));
//                    // verify that the values of the 3 retrieved fields are the correct ones :
//                    Assert.assertEquals("field_beg_1_document1_value", responseRecord.getRetrievedFields().get("field_beg_1").toString());
//                    Assert.assertEquals("field_beg_3_document1_value", responseRecord.getRetrievedFields().get("field_beg_3").toString());
//                    Assert.assertEquals("field_fin_1_document1_value", responseRecord.getRetrievedFields().get("field_fin_1").toString());
//                }
//                if (responseRecord.getDocumentId() == docId2)
//                    Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document2, verify that 3 fields has been retrieved
//                if (responseRecord.getDocumentId() == docId3)
//                    Assert.assertEquals(2, responseRecord.getRetrievedFields().size()); // for document3, verify that 2 fields has been retrieved
//            });
//
//        multiGetQueryRecords.clear();
//        documentIds.clear();
//        multiGetResponseRecords.clear();
//
//        // Test : 2 MultiGetQueryRecord records :
//        //    - 1st : 1 index (index1), 1 type, 2 ids, WITH include, WITH exclude    --> expecting : 2 docs retrieved (from index1), 3 fields each (except doc3 : 2 fields)
//        //    - 2nd : 1 index (index2), 0 type, 3 ids, WITH include, WITHOUT exclude --> expecting : 3 docs retrieved (from index2), 4 fields each (except doc3 : 3 fields)
//        documentIds.add(docId1);
//        documentIds.add(docId2);
//        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, type1, fieldsToInclude, fieldsToExclude, documentIds));
//        documentIds_2.add(docId1);
//        documentIds_2.add(docId1);
//        documentIds_2.add(docId1);
//        multiGetQueryRecords.add(new MultiGetQueryRecord(index2, null , fieldsToInclude, null, documentIds_2));
//        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
//
//        Assert.assertEquals(5, multiGetResponseRecords.size()); // verify that 5 documents has been retrieved
//        multiGetResponseRecords.forEach(responseRecord -> {
//            if (responseRecord.getIndexName() == index1 && !responseRecord.getDocumentId().equals(docId3))
//                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for documents from index1 (except doc3), verify that 3 fields has been retrieved
//            if (responseRecord.getIndexName() == index1 && responseRecord.getDocumentId().equals(docId3))
//                Assert.assertEquals(2, responseRecord.getRetrievedFields().size()); // for document3 from index1, verify that 2 fields has been retrieved
//            if (responseRecord.getDocumentId() == index2 && !responseRecord.getDocumentId().equals(docId3))
//                Assert.assertEquals(4, responseRecord.getRetrievedFields().size()); // for documents from index2 (except doc3), verify that 4 fields has been retrieved
//            if (responseRecord.getDocumentId() == index2 && responseRecord.getDocumentId().equals(docId3))
//                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document3 from index2, verify that 3 fields has been retrieved
//        });

    }

    @Test
    public void testMultiGetInvalidRecords() throws InitializationException, IOException, InterruptedException {

//        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
//
//        String errorMessage ="";
//
//        // Validate null index behaviour :
//        try {
//            multiGetQueryRecords.add(new MultiGetQueryRecord(null, null, null, null, null));
//        }  catch (InvalidMultiGetQueryRecordException e) {
//            errorMessage = e.getMessage();
//        }
//        Assert.assertEquals(errorMessage,"The index name cannot be null");
//
//        // Validate empty index behaviour :
//        try {
//            multiGetQueryRecords.add(new MultiGetQueryRecord("", null, null, null, null));
//        }  catch (InvalidMultiGetQueryRecordException e) {
//            errorMessage = e.getMessage();
//        }
//        Assert.assertEquals(errorMessage,"The index name cannot be empty");
//
//        // Validate null documentIds behaviour :
//        try {
//            multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", null, null, null, null));
//        }  catch (InvalidMultiGetQueryRecordException e) {
//            errorMessage = e.getMessage();
//        }
//        Assert.assertEquals(errorMessage,"The list of document ids cannot be null");
//
//        // Make sure no invalid MultiGetQueryRecord has been added to multiGetQueryRecords list :
//        Assert.assertEquals(0, multiGetQueryRecords.size());
    }
}
