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
package com.hurence.logisland.service.datastore;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;


@Tags({"datastore", "record", "service"})
@CapabilityDescription("A controller service for accessing an abstract datastore.")
public interface DatastoreClientService extends ControllerService {

    PropertyDescriptor FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("flush.interval")
            .description("flush interval in ms")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();

    PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch.size")
            .description("The preferred number of Records to setField to the database in a single transaction")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    PropertyDescriptor BULK_SIZE = new PropertyDescriptor.Builder()
            .name("bulk.size")
            .description("bulk size in MB")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();


    /* ********************************************************************
     * Collection handling section
     * ********************************************************************/

    /**
     * Wait until specified collection is ready to be used.
     */
    void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException;

    /**
     * Wait until specified collection is ready to be used.
     */
    default void waitUntilCollectionReady(String name) throws DatastoreClientServiceException {
        waitUntilCollectionReady(name, 10000L);
    }

    /**
     * Create the specified collection or index or table or bucket.
     * Specify namespace as dotted notation like in `global.users`
     */
    void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException;

    /**
     * Drop the specified collection/index/table/bucket.
     * Specify namespace as dotted notation like in `global.users`
     */
    void dropCollection(String name)throws DatastoreClientServiceException;

    /**
     * Return the number of documents in the collection.
     */
    long countCollection(String name) throws DatastoreClientServiceException;

    /**
     * Return true if the specified collection exists (also true if the name is an alias to an index).
     */
    boolean existsCollection(String name) throws DatastoreClientServiceException;

    /**
     * Wait until the specified collection has integrated all previously-saved data.
     */
    void refreshCollection(String name) throws DatastoreClientServiceException;


    /**
     * Copy the contents of srcIndex into dstIndex.
     * <p>
     * Although ES provides a "reindex" REST endpoint, it does so via a "standard extension module" rather than
     * implementing the logic in ES core itself. This means there is no reindex java API; we must implement
     * reindexing as a search-scroll loop.
     * </p>
     * <p>
     * Credits: http://blog.davidvassallo.me/2016/10/11/elasticsearch-java-tips-for-faster-re-indexing/
     * </p>
     */
    void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException;


    /**
     * Creates an alias.
     */
    void createAlias(String collection, String alias)throws DatastoreClientServiceException;


    /**
     * Adds a mapping to an index, or overwrites an existing mapping.
     * <p>
     * If the new mapping is "not compatible" with the index, then false is returned. If a system-error occurred
     * while updating the index, an exception is thrown.
     * </p>
     */
    boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws DatastoreClientServiceException;




    /* ********************************************************************
     * Put handling section
     * ********************************************************************/

    /**
     * Flush the bulk processor.
     */
    void bulkFlush() throws DatastoreClientServiceException;

    /**
     * Put a given document in bulk.
     *
     * @param collectionName the table or index to put record in
     */
    void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException;

    /**
     * Put the specified Record to the given collection.
     *
     * @param collectionName
     * @param record
     * @param asynchronous
     * @throws Exception
     */
    void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException;


    /**
     * Remove the specified Record from the given collection.
     *
     * @param collectionName
     * @param record
     * @param asynchronous
     * @throws Exception
     */
    void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException;




    /* ********************************************************************
     * Get handling section
     * ********************************************************************/

    /**
     * Get a list of documents based on their index, type and id.
     *
     * @param multiGetQueryRecords list of MultiGetQueryRecord to fetch
     * @return the list of fetched MultiGetResponseRecord records
     */
    List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException;

    /**
     * Get the specified Record to the given collection.
     *
     * @param collectionName
     * @param record
     * @throws Exception
     */
    Record get(String collectionName, Record record) throws DatastoreClientServiceException;

    /**
     * Results of a given search query.
     */
    Collection<Record> query(String query);

    /**
     * Number of Hits of a given search query.
     */
    long queryCount(String query);
}
