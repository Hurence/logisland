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
package com.hurence.logisland.processor.webanalytics;

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.elasticsearch.ElasticsearchClientService;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;

import java.util.*;

public class MockElasticsearchClientService extends AbstractControllerService implements ElasticsearchClientService {

    private class StringDocument {

        private final String docIndex;
        private final String docType;
        private final String document;

        public StringDocument(String docIndex, String docType, String document) {
            this.docIndex = docIndex;
            this.docType = docType;
            this.document = document;
        }

        public String getDocIndex() {
            return docIndex;
        }

        public String getDocType() {
            return docType;
        }

        public String getDocument() {
            return document;
        }
    }

    private class MapDocument {

        private final String docIndex;
        private final String docType;
        private final Map<String, ?> document;

        public MapDocument(String docIndex, String docType, Map<String, ?> document) {
            this.docIndex = docIndex;
            this.docType = docType;
            this.document = document;
        }

        public String getDocIndex() {
            return docIndex;
        }

        public String getDocType() {
            return docType;
        }

        public Map<String, ?> getDocument() {
            return document;
        }
    }

    //private Map<String,Map<String, String>> putStringRecords = new HashMap<>();
    private List<StringDocument> stringDocuments = new ArrayList<>();
    //private Map<String, Map<String, Map<String, ?>>> putMapRecords = new HashMap<>();
    private List<MapDocument> mapDocuments = new ArrayList<>();

    @Override
    public void bulkFlush() {
    }

    @Override
    public void bulkPut(String docIndex, String docType, String document, Optional<String> OptionalId) {
        //Map<String, String> typeAndDocument = new HashMap<>();
        //typeAndDocument.put(docType, document);
        //this.putStringRecords.put(docIndex, typeAndDocument);

        StringDocument stringDocument = new StringDocument(docIndex, docType, document);
        this.stringDocuments.add(stringDocument);
    }

    @Override
    public void bulkPut(String docIndex, String docType, Map<String, ?> document, Optional<String> OptionalId) {
        //Map<String, Map<String, ?>> typeAndDocument = new HashMap<>();
        //typeAndDocument.put(docType, document);
        //this.putMapRecords.put(docIndex, typeAndDocument);

        MapDocument mapDocument = new MapDocument(docIndex, docType, document);
        this.mapDocuments.add(mapDocument);
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) {

        List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();
        multiGetQueryRecords.forEach(multiGetQueryRecord -> {
            String index = multiGetQueryRecord.getIndexName();
            String type = multiGetQueryRecord.getTypeName();
            multiGetQueryRecord.getDocumentIds().forEach(id -> {
                MultiGetResponseRecord multiGetResponseRecord = new MultiGetResponseRecord(index, type, id, null);
                multiGetResponseRecords.add(multiGetResponseRecord);
            });
        });
        return multiGetResponseRecords;
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

    @Override
    public boolean existsCollection(String indexName) throws DatastoreClientServiceException {

        for (int i = 0; i < stringDocuments.size(); i++) {
            if (stringDocuments.get(i).getDocIndex().equals(indexName))
                return true;
        }

        for (int i = 0; i < mapDocuments.size(); i++) {
            if (mapDocuments.get(i).getDocIndex().equals(indexName))
                return true;
        }

        return false;
    }

    @Override
    public void refreshCollection(String indexName) throws DatastoreClientServiceException {
    }


    @Override
    public void saveSync(String indexName, String doctype, Map<String, Object> doc) throws Exception {
    }

    @Override
    public long countCollection(String indexName) throws DatastoreClientServiceException {
        return stringDocuments.size() + mapDocuments.size();
    }

    @Override
    public void createCollection(String indexName, int numShards, int numReplicas) throws DatastoreClientServiceException {
    }

    @Override
    public void dropCollection(String indexName) throws DatastoreClientServiceException {
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String srcIndex, String dstIndex)
            throws DatastoreClientServiceException {
    }


    @Override
    public void createAlias(String indexName, String aliasName) throws DatastoreClientServiceException {
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws DatastoreClientServiceException {
        return true;
    }


    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {

    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {

    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {

    }

    @Override
    public String convertRecordToString(Record record) {
        return record.toString();
    }

    @Override
    public long searchNumberOfHits(String docIndex, String docType, String docName, String docValue) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();

        return Collections.unmodifiableList(props);
    }
}
