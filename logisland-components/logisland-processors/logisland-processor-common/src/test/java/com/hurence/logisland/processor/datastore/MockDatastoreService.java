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
package com.hurence.logisland.processor.datastore;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiGetResponseRecord;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;

import java.util.*;

public class MockDatastoreService implements DatastoreClientService {

    Map<String, Map<String, Record>> collections = new HashMap<>();

    @Override
    public void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException {

    }

    @Override
    public void waitUntilCollectionReady(String name) throws DatastoreClientServiceException {

    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {
        collections.computeIfAbsent(name, k -> new HashMap<>());
    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {
        collections.remove(name);
    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        return collections.get(name).size();
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        return collections.get(name) != null;
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {

    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        collections.put(dst, new HashMap<>(collections.get(src)));
    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {

    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        return false;
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {

    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        put(collectionName, record, false);
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        if (!existsCollection(collectionName))
            createCollection(collectionName, 0, 0);
        collections.get(collectionName).put(record.getId(), record);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        collections.get(collectionName).remove(record.getId());
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        List<MultiGetResponseRecord> results = new ArrayList<>();
        for (MultiGetQueryRecord mgqr : multiGetQueryRecords) {
            String collectionName = mgqr.getIndexName();
            String typeName = mgqr.getTypeName();
            for (String id : mgqr.getDocumentIds()) {
                Record record = get(collectionName, new StandardRecord().setId(id));
                Map<String, String> retrievedFields = new HashMap<>();
                if (record != null) {

                    if (mgqr.getFieldsToInclude()[0].equals("*")) {
                        for (Field field : record.getAllFieldsSorted()) {
                            retrievedFields.put(field.getName(), field.asString());
                        }
                    }else{
                        for (String prop : mgqr.getFieldsToInclude()) {
                            retrievedFields.put(prop, record.getField(prop).asString());
                        }
                    }
                }
                results.add(new MultiGetResponseRecord(collectionName, typeName, id, retrievedFields));
            }
        }

        return results;
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        return collections.get(collectionName).get(record.getId());
    }

    @Override
    public Collection<Record> query(String query) {
        return Collections.emptyList();
    }

    @Override
    public long queryCount(String query) {
        return 0;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {

    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return Collections.emptyList();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) {
        return null;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    }

    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public String getIdentifier() {
        return "MockDatastoreService";
    }
}
