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

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.MultiQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiQueryResponseRecord;
import com.hurence.logisland.service.datastore.model.QueryRecord;
import com.hurence.logisland.service.datastore.model.QueryResponseRecord;
import com.hurence.logisland.service.datastore.model.bool.BoolCondition;
import com.hurence.logisland.service.datastore.model.bool.WildCardQueryRecord;
import com.hurence.logisland.service.datastore.model.exception.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.datastore.model.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiGetResponseRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hurence.logisland.service.elasticsearch.ElasticsearchClientService.*;

/**
 * The current implementation uses HTTPS with no server certificate validation (like the ES service does) as well as
 * user/password http basic auth, which is currently only admin/admin as it is by default configured in the opendistro
 * ES docker image we currently use.
 */
public class ElasticsearchOpenDistro_7_x_ClientServiceIT {

    private static final String MAPPING1 = "{'properties':{'name':{'type': 'text'},'val':{'type':'integer'}}}";
    private static final String MAPPING2 = "{'properties':{'name':{'type': 'text'},'val':{'type': 'text'}}}";
    private static final String MAPPING3 =
            "{'dynamic':'strict','properties':{'name':{'type': 'text'},'xyz':{'type': 'text'}}}";

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchOpenDistro_7_x_ClientServiceIT.class);

    // For the moment, the ES opendistro container does not support configuring and using another user/password than
    // admin/admin. To be allowed to changed that, the ElasticsearchOpenDistroContainer constructor must find a way
    // to configure a new user/password starting the opendistro container.
    public static final String OPENDISTRO_USERNAME = "admin";
    public static final String OPENDISTRO_PASSWORD = "admin";

    @ClassRule
    public static final ESOpenDistroRule esOpenDistroRule = new ESOpenDistroRule(OPENDISTRO_USERNAME, OPENDISTRO_PASSWORD);

    @After
    public void clean() throws IOException {
//        ClusterHealthRequest is returning nothing... So we are using IndexRequest here
        GetIndexRequest request = new GetIndexRequest("*");
        GetIndexResponse response;
        try {
            response = esOpenDistroRule.getClient().indices().get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ex) {
            return;//should be index not found
        }
        String[] indices = response.getIndices();
        List<String> indicesToClean = new ArrayList<String>();
        // Do not remove .opendistro_security mandatory index
        Arrays.stream(indices).forEach(index -> {
            if (!index.equals(".opendistro_security")) {
                indicesToClean.add(index);
            }
        });
        if (indicesToClean.size() > 0) {
            logger.info("Cleaning indices:" + indicesToClean);
            DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indicesToClean.toArray(new String[0]));
            Assert.assertTrue(esOpenDistroRule.getClient().indices().delete(deleteRequest, RequestOptions.DEFAULT).isAcknowledged());
        } else {
            logger.info("No index to clean");
        }
    }

    private static ElasticsearchClientService configureElasticsearchOpenDistroClientService(final TestRunner runner,
                                                                                            final Map<PropertyDescriptor, String>... extraProperties) throws InitializationException {
        // Use default implementation of service.
        return configureElasticsearchOpenDistroClientService(new Elasticsearch_7_x_ClientService(), runner, extraProperties);
    }

    private static ElasticsearchClientService configureElasticsearchOpenDistroClientService(final Elasticsearch_7_x_ClientService elasticsearchClientService,
                                                                                            final TestRunner runner,
                                                                                            final Map<PropertyDescriptor, String>... extraProperties) throws InitializationException {

        runner.addControllerService("elasticsearchClient", elasticsearchClientService);
        runner.setProperty(TestProcessor.ELASTICSEARCH_CLIENT_SERVICE, "elasticsearchClient");
        runner.setProperty(elasticsearchClientService, HOSTS, esOpenDistroRule.getHostPortString());
        runner.setProperty(elasticsearchClientService, USERNAME, OPENDISTRO_USERNAME);
        runner.setProperty(elasticsearchClientService, PASSWORD, OPENDISTRO_PASSWORD);
        runner.setProperty(elasticsearchClientService, ENABLE_SSL, "true");
        // Set extra properties
        Arrays.stream(extraProperties).forEach(map->map.forEach((key,value)->runner.setProperty(elasticsearchClientService, key, value)));
        runner.enableControllerService(elasticsearchClientService);

        // TODO : is this necessary ?
        final ElasticsearchClientService service = PluginProxy.unwrap(runner.getProcessContext().getPropertyValue(TestProcessor.ELASTICSEARCH_CLIENT_SERVICE).asControllerService());
        return service;
    }

    @Test
    public void testBasics() throws Exception {

        Map<String, Object> document1 = new HashMap<>();
        document1.put("name", "fred");
        document1.put("val", 33);

        boolean result;

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchOpenDistroClientService(runner);


        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsCollection("foo"));

        // Define the index
        elasticsearchClientService.createCollection("foo", 2, 1);
        Assert.assertEquals(true, elasticsearchClientService.existsCollection("foo"));

        // Define another index
        elasticsearchClientService.createCollection("bar", 2, 1);
        Assert.assertEquals(true, elasticsearchClientService.existsCollection("foo"));

        // Add a mapping to foo
        result = elasticsearchClientService.putMapping("foo", null, MAPPING1.replace('\'', '"'));
        Assert.assertEquals(true, result);

        // Add the same mapping again
        result = elasticsearchClientService.putMapping("foo", null, MAPPING1.replace('\'', '"'));
        Assert.assertEquals(true, result);

        // create alias
        elasticsearchClientService.createAlias("foo", "aliasFoo");
        Assert.assertEquals(true, elasticsearchClientService.existsCollection("aliasFoo"));

        // Insert a record into foo and count foo
        Assert.assertEquals(0, elasticsearchClientService.countCollection("foo"));
        elasticsearchClientService.saveSync("foo", null, document1);
        Assert.assertEquals(1, elasticsearchClientService.countCollection("foo"));

        // copy index foo to bar - should work
        Assert.assertEquals(0, elasticsearchClientService.countCollection("bar"));
        elasticsearchClientService.copyCollection(TimeValue.timeValueMinutes(2).toString(), "foo", "bar");
        elasticsearchClientService.bulkFlush();
        Thread.sleep(2000);
        elasticsearchClientService.refreshCollection("bar");
        Assert.assertEquals(1, elasticsearchClientService.countCollection("bar"));

        // Define incompatible mappings in two different indexes, then try to copy - should fail
        // as a document registered in index foo cannot be written in index baz.
        //
        // Note: MAPPING2 cannot be added to index foo or bar at all, even under a different doctype, as ES (lucene)
        // does not allow two types for the same field-name in different mappings of the same index. However if
        // MAPPING2 is added to index baz, then the copyCollection succeeds - because by default ES automatically converts
        // integers into strings when necessary. Interestingly, this means MAPPING1 and MAPPING2 are not compatible
        // at the "put mapping" level, but are compatible at the "reindex" level..
        //
        // The document (doc1) of type "type1" already in index "foo" cannot be inserted into index "baz" as type1
        // because that means applying its source to MAPPING3 - but MAPPING3 is strict and does not define property
        // "val", so the insert fails.
        elasticsearchClientService.createCollection("baz",2, 1);
        elasticsearchClientService.putMapping("baz", null, MAPPING2.replace('\'', '"'));

//       try {
//            elasticsearchClientService.copyCollection(TimeValue.timeValueMinutes(2).toString(), "foo", "baz");
//            Assert.fail("Exception not thrown when expected");
//        } catch(DatastoreClientServiceException e) {
//            Assert.assertTrue(e.getMessage().contains("Reindex failed"));
//        }
        elasticsearchClientService.refreshCollection("baz");
        Assert.assertEquals(0, elasticsearchClientService.countCollection("baz"));

        // Drop index foo
        elasticsearchClientService.dropCollection("foo");
        Assert.assertEquals(false, elasticsearchClientService.existsCollection("foo"));
        Assert.assertEquals(false, elasticsearchClientService.existsCollection("aliasFoo")); // alias for foo disappears too
        Assert.assertEquals(true, elasticsearchClientService.existsCollection("bar"));
    }

    @Test
    public void testBulkPut() throws InitializationException, IOException, InterruptedException {
        final String index = "foo";
        final String docId = "id1";
        final String nameKey = "name";
        final String nameValue = "fred";
        final String ageKey = "age";
        final int ageValue = 33;

        Map<String, Object> document1 = new HashMap<>();
        document1.put(nameKey, nameValue);
        document1.put(ageKey, ageValue);

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchOpenDistroClientService(runner);

        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsCollection(index));

        // Create the index
        elasticsearchClientService.createCollection(index,2, 1);
        Assert.assertEquals(true, elasticsearchClientService.existsCollection(index));

        // Put a document in the bulk processor :
        elasticsearchClientService.bulkPut(index, null, document1, Optional.of(docId));
        // Flush the bulk processor :
        elasticsearchClientService.bulkFlush();
        Thread.sleep(2000);
        try {
            // Refresh the index :
            elasticsearchClientService.refreshCollection(index);
        } catch (Exception e) {
            logger.error("Error while refreshing the index : " + e.toString());
        }

        long documentsNumber = 0;

        try {
            documentsNumber = elasticsearchClientService.countCollection(index);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(1, documentsNumber);

        try {
            elasticsearchClientService.saveSync(index, null, document1);
        } catch (Exception e) {
            logger.error("Error while saving the document in the index : " + e.toString());
        }

        try {
            documentsNumber = elasticsearchClientService.countCollection(index);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(2, documentsNumber);

        long numberOfHits = elasticsearchClientService.searchNumberOfHits(index, null, nameKey, nameValue);

        Assert.assertEquals(2, numberOfHits);

    }


    @Test
    public void testBulkPutGeopoint() throws InitializationException, InterruptedException {
        final String index = "future_factory";
        final String docId = "modane_factory";
        Record record = new StandardRecord("factory")
                .setId(docId)
                .setStringField("address", "rue du Frejus")
                .setField("latitude", FieldType.FLOAT, 45.4f)
                .setField("longitude", FieldType.FLOAT, 45.4f);

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchOpenDistroClientService(runner);

        // Verify the index does not exist
        Assert.assertEquals(false, elasticsearchClientService.existsCollection(index));

        // Create the index
        elasticsearchClientService.createCollection(index, 2, 1);
        Assert.assertEquals(true, elasticsearchClientService.existsCollection(index));

        // Put a document in the bulk processor :
        String document1 = ElasticsearchRecordConverter.convertToString(record);
        elasticsearchClientService.bulkPut(index, null, document1, Optional.of(docId));
        // Flush the bulk processor :
        elasticsearchClientService.bulkFlush();
        Thread.sleep(2000);
        try {
            // Refresh the index :
            elasticsearchClientService.refreshCollection(index);
        } catch (Exception e) {
            logger.error("Error while refreshing the index : " + e.toString());
        }

        long documentsNumber = 0;

        try {
            documentsNumber = elasticsearchClientService.countCollection(index);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }

        Assert.assertEquals(1, documentsNumber);

        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
        ArrayList<String> documentIds = new ArrayList<>();
        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();


        // Make sure a dummy query returns no result :
        documentIds.add(docId);
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord(index, null, new String[]{"location", "id"}, new String[]{}, documentIds));
        } catch (InvalidMultiGetQueryRecordException e) {
            e.printStackTrace();
        }
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
        Assert.assertEquals(1, multiGetResponseRecords.size()); // number of documents retrieved

    }


    @Test
    public void testMultiGet() throws InitializationException, InterruptedException, InvalidMultiGetQueryRecordException {
        final String index1 = "index1";
        final String index2 = "index2";

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

        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        // create the controller service and link it to the test processor :
        final ElasticsearchClientService elasticsearchClientService = configureElasticsearchOpenDistroClientService(runner);

        // Verify the indexes do not exist
        Assert.assertEquals(false, elasticsearchClientService.existsCollection(index1));
        Assert.assertEquals(false, elasticsearchClientService.existsCollection(index2));

        // Create the indexes
        elasticsearchClientService.createCollection(index1, 2, 1);
        elasticsearchClientService.createCollection(index2, 2, 1);
        Assert.assertEquals(true, elasticsearchClientService.existsCollection(index1));
        Assert.assertEquals(true, elasticsearchClientService.existsCollection(index2));

        // Put documents in the bulk processor :
        elasticsearchClientService.bulkPut(index1, null, document1, Optional.of(docId1));
        elasticsearchClientService.bulkPut(index1, null, document2, Optional.of(docId2));
        elasticsearchClientService.bulkPut(index1, null, document3, Optional.of(docId3));
        elasticsearchClientService.bulkPut(index2, null, document1, Optional.of(docId1));
        elasticsearchClientService.bulkPut(index2, null, document2, Optional.of(docId2));
        elasticsearchClientService.bulkPut(index2, null, document3, Optional.of(docId3));
        // Flush the bulk processor :
        elasticsearchClientService.bulkFlush();
        Thread.sleep(2000);
        try {
            // Refresh the indexes :
            elasticsearchClientService.refreshCollection(index1);
            elasticsearchClientService.refreshCollection(index2);
        } catch (Exception e) {
            logger.error("Error while refreshing the indexes : " + e.toString());
        }

        long countIndex1 = 0;
        long countIndex2 = 0;
        try {
            countIndex1 = elasticsearchClientService.countCollection(index1);
            countIndex2 = elasticsearchClientService.countCollection(index2);
        } catch (Exception e) {
            logger.error("Error while counting the number of documents in the index : " + e.toString());
        }
        Assert.assertEquals(3, countIndex1);
        Assert.assertEquals(3, countIndex2);

        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
        ArrayList<String> documentIds = new ArrayList<>();
        ArrayList<String> documentIds_2 = new ArrayList<>();
        List<MultiGetResponseRecord> multiGetResponseRecords;
        String[] fieldsToInclude = {"field_b*", "field*1"};
        String[] fieldsToExclude = {"field_*2"};

        // Make sure a dummy query returns no result :
        documentIds.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", "", new String[]{"dummy"}, new String[]{}, documentIds));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);
        Assert.assertEquals(0, multiGetResponseRecords.size()); // number of documents retrieved

        multiGetQueryRecords.clear();
        documentIds.clear();
        multiGetResponseRecords.clear();

        // Test : 1 MultiGetQueryRecord record, with 1 index, 1 type, 1 id, WITHOUT includes, WITHOUT excludes :
        documentIds.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, null, documentIds));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(1, multiGetResponseRecords.size()); // number of documents retrieved
        Assert.assertEquals(index1, multiGetResponseRecords.get(0).getCollectionName());
        Assert.assertEquals("_doc", multiGetResponseRecords.get(0).getTypeName());
        Assert.assertEquals(docId1, multiGetResponseRecords.get(0).getDocumentId());
        Assert.assertEquals(5, multiGetResponseRecords.get(0).getRetrievedFields().size()); // number of fields retrieved for the document
        multiGetResponseRecords.get(0).getRetrievedFields().forEach((k, v) -> document1.get(k).equals(v.toString()));

        multiGetQueryRecords.clear();
        documentIds.clear();
        multiGetResponseRecords.clear();

        // Test : 1 MultiGetQueryRecord record, with 1 index, 0 type, 3 ids, WITH include, WITH exclude :
        documentIds.add(docId1);
        documentIds.add(docId2);
        documentIds.add(docId3);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, null, fieldsToInclude, fieldsToExclude, documentIds));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(3, multiGetResponseRecords.size()); // verify that 3 documents has been retrieved
        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals(index1, responseRecord.getCollectionName())); // verify that all retrieved are in index1
        multiGetResponseRecords.forEach(responseRecord -> Assert.assertEquals("_doc", responseRecord.getTypeName())); // verify that the type of all retrieved documents is type1
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
        multiGetQueryRecords.add(new MultiGetQueryRecord(index1, null, fieldsToInclude, fieldsToExclude, documentIds));
        documentIds_2.add(docId1);
        documentIds_2.add(docId1);
        documentIds_2.add(docId1);
        multiGetQueryRecords.add(new MultiGetQueryRecord(index2, null, fieldsToInclude, null, documentIds_2));
        multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

        Assert.assertEquals(5, multiGetResponseRecords.size()); // verify that 5 documents has been retrieved
        multiGetResponseRecords.forEach(responseRecord -> {
            if (responseRecord.getCollectionName() == index1 && !responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for documents from index1 (except doc3), verify that 3 fields has been retrieved
            if (responseRecord.getCollectionName() == index1 && responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(2, responseRecord.getRetrievedFields().size()); // for document3 from index1, verify that 2 fields has been retrieved
            if (responseRecord.getDocumentId() == index2 && !responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(4, responseRecord.getRetrievedFields().size()); // for documents from index2 (except doc3), verify that 4 fields has been retrieved
            if (responseRecord.getDocumentId() == index2 && responseRecord.getDocumentId().equals(docId3))
                Assert.assertEquals(3, responseRecord.getRetrievedFields().size()); // for document3 from index2, verify that 3 fields has been retrieved
        });

    }

    @Test
    public void testMultiQueryGet() throws InitializationException, InterruptedException {

        final String index = "index";
        final int numDocs = 55;
        final int chunkSize = 10;
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());

        // create the controller service and link it to the test processor :
        final AtomicInteger call_num = new AtomicInteger();
        final Elasticsearch_7_x_ClientService es = new Elasticsearch_7_x_ClientService() {
            // Override default implementation to track the number of actual calls.
            @Override
            Collection<QueryResponseRecord> _multiQueryGet(MultiQueryRecord queryRecords) throws DatastoreClientServiceException {
                call_num.incrementAndGet();
                return super._multiQueryGet(queryRecords);
            }
        };
        final ElasticsearchClientService elasticsearchClientService =
            configureElasticsearchOpenDistroClientService(es, runner, Collections.singletonMap(MULTIGET_MAXQUERIES, String.valueOf(chunkSize)));
        // Create docs and one query per doc.
        final List<QueryRecord> queries = new ArrayList<>();
        for(int i=0;i<numDocs;i++) {
            elasticsearchClientService.bulkPut(index, null, Collections.singletonMap("field", String.valueOf(i)), Optional.of("docId"+i));
            queries.add(new QueryRecord().addCollection("*")
                                         .addBoolQuery(new WildCardQueryRecord("field", String.valueOf(i)),
                                                       BoolCondition.MUST));
        }
        // Flush the bulk processor :
        elasticsearchClientService.bulkFlush();
        Thread.sleep(2000);
        // Refresh the indexes :
        elasticsearchClientService.refreshCollection(index);
        // Perform actual test.
        elasticsearchClientService.multiQueryGet(new MultiQueryRecord(queries));
        // Check the number of expected calls.
        Assert.assertEquals(numDocs/chunkSize+(numDocs%chunkSize==0?0:1), call_num.get());
    }

    @Test
    public void testMultiGetInvalidRecords() {

        List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();

        String errorMessage = "";

        // Validate null index behaviour :
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord(null, null, null, null, null));
        } catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertEquals(errorMessage, "The index name cannot be null");

        // Validate empty index behaviour :
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord("", null, null, null, null));
        } catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertEquals(errorMessage, "The index name cannot be empty");

        // Validate null documentIds behaviour :
        try {
            multiGetQueryRecords.add(new MultiGetQueryRecord("dummy", null, null, null, null));
        } catch (InvalidMultiGetQueryRecordException e) {
            errorMessage = e.getMessage();
        }
        Assert.assertEquals(errorMessage, "The list of document ids cannot be null");

        // Make sure no invalid MultiGetQueryRecord has been added to multiGetQueryRecords list :
        Assert.assertEquals(0, multiGetQueryRecords.size());
    }
}
