package com.hurence.logisland.processor.webanalytics.util;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.webanalytics.MockCacheService;
import com.hurence.logisland.processor.webanalytics.modele.FirstUserVisitCompositeKey;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.*;
import com.hurence.logisland.service.datastore.model.bool.BoolCondition;
import com.hurence.logisland.service.datastore.model.bool.BoolNode;
import com.hurence.logisland.service.datastore.model.bool.BoolQueryRecord;
import com.hurence.logisland.service.datastore.model.bool.TermQueryRecord;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.validator.Configuration;
import com.hurence.logisland.validator.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class FirstUserVisitTimestampManagerImplTest {

    @Test
    public void nominalCase() {
        AbstractMockEsQueryGetService elasticsearchClientService = new AbstractMockEsQueryGetService() {
            @Override
            public QueryResponseRecord queryGet(QueryRecord queryRecord) throws DatastoreClientServiceException {
                this.queryGetMethodCallCount++;
                // validate that the QueryRecord object was correctly built by FirstUserVisitTimestampManagerImpl
                assertNotNull(queryRecord);

                assertEquals(0, queryRecord.getSize());

                assertNotNull(queryRecord.getBoolQuery());
                List<BoolNode<BoolQueryRecord>> boolNodes = queryRecord.getBoolQuery().getChildren();
                assertEquals(3, boolNodes.size());
                for (BoolNode<BoolQueryRecord> boolNode: boolNodes) {
                    assertEquals(BoolCondition.MUST, boolNode.getBoolCondition());
                    assertTrue(boolNode.getChildren().isEmpty());
                    assertTrue(boolNode.getData() instanceof TermQueryRecord);
                    TermQueryRecord termQueryRecord = (TermQueryRecord) boolNode.getData();
                    Map<String, String> fieldMap = new HashMap<>();
                    fieldMap.put("userId.raw", "userA");
                    fieldMap.put("someFieldA.raw", "someFieldValueA");
                    fieldMap.put("someFieldB.raw", "someFieldValueB");
                    assertTrue(fieldMap.containsKey(termQueryRecord.getFieldName()));
                    assertEquals(fieldMap.get(termQueryRecord.getFieldName()), termQueryRecord.getFieldValue());
                }

                List<AggregationRecord> aggregationQueries = queryRecord.getAggregationQueries();
                assertNotNull(aggregationQueries);
                assertEquals(1, aggregationQueries.size());
                AggregationRecord aggregationRecord = aggregationQueries.get(0);
                assertEquals("minAgg", aggregationRecord.getAggregationName());
                assertEquals("firstEventEpochSeconds", aggregationRecord.getFieldName());

                AggregationResponseRecord aggregationResponseRecord = new MinAggregationResponseRecord("minAgg", "min", 1641548905000L);
                return new QueryResponseRecord(0, Collections.emptyList(), Collections.singletonList(aggregationResponseRecord));
            }
        };
        MockCacheService<FirstUserVisitCompositeKey, Long> cacheService = new MockCacheService<>();
        Map<String, String> additionalFieldMapForFirstUserVisitKey = new HashMap<>();
        additionalFieldMapForFirstUserVisitKey.put("someFieldA", "someFieldA.raw");
        additionalFieldMapForFirstUserVisitKey.put("someFieldB", "someFieldB.raw");
        FirstUserVisitTimestampManagerImpl firstUserVisitTimestampManager = new FirstUserVisitTimestampManagerImpl(elasticsearchClientService, "", cacheService, additionalFieldMapForFirstUserVisitKey);
        Map<String, String> additionalAttributeMap = new HashMap<>();
        additionalAttributeMap.put("someFieldA", "someFieldValueA");
        additionalAttributeMap.put("someFieldB", "someFieldValueB");
        assertEquals(0, cacheService.getGetMethodCallCount());
        assertEquals(0, elasticsearchClientService.getQueryGetMethodCallCount());
        Long timestamp = firstUserVisitTimestampManager.getFirstUserVisitTimestamp(new FirstUserVisitCompositeKey("userA", additionalAttributeMap), null);
        assertEquals(1641548905000L, timestamp);
        assertEquals(1, cacheService.getGetMethodCallCount());
        assertEquals(1, elasticsearchClientService.getQueryGetMethodCallCount());
        timestamp = firstUserVisitTimestampManager.getFirstUserVisitTimestamp(new FirstUserVisitCompositeKey("userA", additionalAttributeMap), 1541548905000L);
        assertEquals(1541548905000L, timestamp);
        assertEquals(2, cacheService.getGetMethodCallCount());
        assertEquals(1, elasticsearchClientService.getQueryGetMethodCallCount());
    }


    abstract static class AbstractMockEsQueryGetService implements ElasticsearchClientService {
        protected int queryGetMethodCallCount = 0;
        @Override
        public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {}
        @Override
        public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {}
        @Override
        public void bulkDelete(String docIndex, String docType, String id) {}
        @Override
        public void deleteByQuery(QueryRecord queryRecord) throws DatastoreClientServiceException {}
        @Override
        public MultiQueryResponseRecord multiQueryGet(MultiQueryRecord queryRecords) throws DatastoreClientServiceException {
            return null;
        }
        @Override
        public void waitUntilCollectionIsReadyAndRefreshIfAnyPendingTasks(String[] indices, long timeoutMilli) throws DatastoreClientServiceException {}
        @Override
        public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {}
        @Override
        public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) {
            return 0;
        }
        @Override
        public String convertRecordToString(Record record) {
            return null;
        }
        @Override
        public Collection<ValidationResult> validate(Configuration context) {
            return null;
        }
        @Override
        public PropertyDescriptor getPropertyDescriptor(String name) {
            return null;
        }
        @Override
        public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}
        @Override
        public List<PropertyDescriptor> getPropertyDescriptors() {
            return null;
        }
        @Override
        public String getIdentifier() {
            return null;
        }
        @Override
        public void initialize(ControllerServiceInitializationContext context) throws InitializationException {}
        @Override
        public void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException {}
        @Override
        public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {}
        @Override
        public void dropCollection(String name) throws DatastoreClientServiceException {}
        @Override
        public long countCollection(String name) throws DatastoreClientServiceException {
            return 0;
        }
        @Override
        public boolean existsCollection(String name) throws DatastoreClientServiceException {
            return false;
        }
        @Override
        public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {}
        @Override
        public void createAlias(String collection, String alias) throws DatastoreClientServiceException {}
        @Override
        public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
            return false;
        }
        @Override
        public void bulkFlush() throws DatastoreClientServiceException {}
        @Override
        public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {}
        @Override
        public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {}
        @Override
        public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {}
        @Override
        public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
            return null;
        }
        @Override
        public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
            return null;
        }
        @Override
        public Collection<Record> query(String query) {
            return null;
        }
        @Override
        public long queryCount(String query) {
            return 0;
        }
        public int getQueryGetMethodCallCount() {
            return this.queryGetMethodCallCount;
        }
    }
}
