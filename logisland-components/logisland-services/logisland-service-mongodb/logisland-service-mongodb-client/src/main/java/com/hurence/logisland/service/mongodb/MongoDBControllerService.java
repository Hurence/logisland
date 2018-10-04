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
package com.hurence.logisland.service.mongodb;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Tags({"mongo", "mongodb", "service"})
@CapabilityDescription(
        "Provides a controller service that wraps most of the functionality of the MongoDB driver."
)
public class MongoDBControllerService extends AbstractMongoDBControllerService implements MongoDBClientService {
    private MongoDatabase db;
    private MongoCollection<Document> col;
    private MongoDBUpdater updater;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    final BlockingQueue<Record> queue = new ArrayBlockingQueue<>(100000);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();

        descriptors.add(URI);
        descriptors.add(DATABASE_NAME);
        descriptors.add(COLLECTION_NAME);
        descriptors.add(BATCH_SIZE);
        descriptors.add(BULK_SIZE);
        descriptors.add(BULK_MODE);
        descriptors.add(FLUSH_INTERVAL);
        descriptors.add(WRITE_CONCERN);
       /* descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(CLIENT_AUTH);*/
        return descriptors;
    }


    // public void init(ControllerServiceInitializationContext context) throws InitializationException {


    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException {
        try {
            this.createClient(context);
        } catch (IOException e) {
            throw new InitializationException(e);
        }
        this.db = this.mongoClient.getDatabase(context.getPropertyValue(MongoDBControllerService.DATABASE_NAME).asString());
        this.col = this.db.getCollection(context.getPropertyValue(MongoDBControllerService.COLLECTION_NAME).asString());
        if (updater != null) {
            return;
        }

        // setup a thread pool of solr updaters
        int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();
        long flushInterval = context.getPropertyValue(FLUSH_INTERVAL).asLong();
        String bulkMode = context.getPropertyValue(BULK_MODE).asString();
        updater = new MongoDBUpdater(db, col, queue, batchSize, flushInterval, bulkMode);
        executorService.execute(updater);
    }

    @OnDisabled
    public void onDisable() {
        this.mongoClient.close();
    }

    @Override
    public long count(Document query) {
        return this.col.count(query);
    }

    @Override
    public void delete(Document query) {
        this.col.deleteMany(query);
    }

    @Override
    public boolean exists(Document query) {
        return this.col.count(query) > 0;
    }

    @Override
    public Document findOne(Document query) {
        MongoCursor<Document> cursor = this.col.find(query).limit(1).iterator();
        Document retVal = cursor.tryNext();
        cursor.close();

        return retVal;
    }

    @Override
    public Document findOne(Document query, Document projection) {
        MongoCursor<Document> cursor = projection != null
                ? this.col.find(query).projection(projection).limit(1).iterator()
                : this.col.find(query).limit(1).iterator();
        Document retVal = cursor.tryNext();
        cursor.close();

        return retVal;
    }

    @Override
    public List<Document> findMany(Document query) {
        return findMany(query, null, -1);
    }

    @Override
    public List<Document> findMany(Document query, int limit) {
        return findMany(query, null, limit);
    }

    @Override
    public List<Document> findMany(Document query, Document sort, int limit) {
        FindIterable<Document> fi = this.col.find(query);
        if (limit > 0) {
            fi = fi.limit(limit);
        }
        if (sort != null) {
            fi = fi.sort(sort);
        }
        MongoCursor<Document> cursor = fi.iterator();
        List<Document> retVal = new ArrayList<>();
        while (cursor.hasNext()) {
            retVal.add(cursor.next());
        }
        cursor.close();

        return retVal;
    }

    @Override
    public void insert(Document doc) {
        this.col.insertOne(doc);
    }

    @Override
    public void insert(List<Document> docs) {
        this.col.insertMany(docs);
    }

    @Override
    public void update(Document query, Document update, boolean multiple) {
        if (multiple) {
            this.col.updateMany(query, update);
        } else {
            this.col.updateOne(query, update);
        }
    }

    @Override
    public void update(Document query, Document update) {
        update(query, update, true);
    }

    @Override
    public void updateOne(Document query, Document update) {
        this.update(query, update, false);
    }

    @Override
    public void upsert(Document query, Document update) {
        this.col.updateOne(query, update, new UpdateOptions().upsert(true));
    }

    @Override
    public void dropDatabase() {
        this.db.drop();
        this.col = null;
    }

    @Override
    public void dropCollection() {
        this.col.drop();
        this.col = null;
    }

    @Override
    public MongoDatabase getDatabase(String name) {
        return mongoClient.getDatabase(name);
    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {

    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {
        this.dropCollection();
    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        return this.count(new Document());
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        return this.col != null;
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        getLogger().warn("not implemented for Mongo");
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        getLogger().warn("not implemented for Mongo");
    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {
        getLogger().warn("not implemented for Mongo");
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        getLogger().warn("not implemented for Mongo");
        return false;
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        if (record != null)
            queue.add(record);
        else
            getLogger().debug("trying to add null record in the queue");
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        this.insert(RecordConverter.convert(record));
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        this.delete(RecordConverter.convert(record));
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {

/**        List<Document> documents = new ArrayList<>();
 multiGetQueryRecords.forEach(record -> record.getDocumentIds()
 .forEach( id -> documents.add(new Document("_id", new ObjectId(id)))));


 return RecordConverter.convert(this.fi));*/

        throw new UnsupportedOperationException("TODO multiGet");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        return RecordConverter.convert(this.findOne(RecordConverter.convert(record)));
    }

    @Override
    public Collection<Record> query(String query) {
        throw new UnsupportedOperationException("TODO query");
        //return null;
    }

    @Override
    public long queryCount(String query) {
        throw new UnsupportedOperationException("TODO queryCount");
    }
}
