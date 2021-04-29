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
package com.hurence.logisland.service.solr.api;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.model.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Tags({ "solr", "client"})
@CapabilityDescription("Abstract implementation of SolrClientService for Solr")
abstract public class SolrClientService extends AbstractControllerService implements DatastoreClientService {

    protected volatile SolrClient solrClient;
    protected SolrRecordConverter converter;
    protected int schemaUpdateTimeout;

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(SolrClientService.class);
    List<SolrUpdater> updaters = null;
    final BlockingQueue<Record> queue = new ArrayBlockingQueue<>(1000000);

    PropertyDescriptor SOLR_CLOUD = new PropertyDescriptor.Builder()
            .name("solr.cloud")
            .description("is slor cloud enabled")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    PropertyDescriptor SOLR_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("solr.connection.string")
            .description("zookeeper quorum host1:2181,host2:2181 for solr cloud or http address of a solr core ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost:8983/solr")
            .build();

    PropertyDescriptor SOLR_COLLECTION = new PropertyDescriptor.Builder()
            .name("solr.collection")
            .description("name of the collection to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor CONCURRENT_REQUESTS = new PropertyDescriptor.Builder()
            .name("solr.concurrent.requests")
            .description("setConcurrentRequests")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("2")
            .build();

    PropertyDescriptor FLUSH_INTERVAL = new PropertyDescriptor.Builder()
            .name("flush.interval")
            .description("flush interval in ms")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .build();

    PropertyDescriptor SCHEMA_UPDATE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("schema.update_timeout")
            .description("Schema update timeout interval in s")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("15")
            .build();

    public SolrRecordConverter getConverter() {
        if (converter == null) {
            converter = new SolrRecordConverter();
        }

        return converter;
    }

    public void setConverter(SolrRecordConverter converter) {
        this.converter = converter;
    }

    public Boolean isCloud() {
        return solrClient instanceof CloudSolrClient;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(BATCH_SIZE);
        props.add(BULK_SIZE);
        props.add(SOLR_CLOUD);
        props.add(SOLR_COLLECTION);
        props.add(SOLR_CONNECTION_STRING);
        props.add(CONCURRENT_REQUESTS);
        props.add(FLUSH_INTERVAL);
        props.add(SCHEMA_UPDATE_TIMEOUT);

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        super.init(context);
        synchronized(this) {
            try {
                setClient(createSolrClient(context));
            }catch (Exception e){
                throw new InitializationException(e);
            }
        }
    }

    abstract protected SolrClient createCloudClient(String connectionString, String collection);
    abstract protected SolrClient createHttpClient(String connectionString, String collection);

    public void setSchemaUpdateTimeout(int schemaUpdateTimeout) {
        this.schemaUpdateTimeout = schemaUpdateTimeout;
    }

    public int getSchemaUpdateTimeout() {
        return schemaUpdateTimeout;
    }

    /**
     * Instantiate ElasticSearch Client. This chould be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected SolrClient createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
        if (solrClient != null) {
            return solrClient;
        }
        try {
            SolrClient client;
            // create a solr client
            final Boolean isCloud = context.getPropertyValue(SOLR_CLOUD).asBoolean();
            final String connectionString = context.getPropertyValue(SOLR_CONNECTION_STRING).asString();
            final String collection = context.getPropertyValue(SOLR_COLLECTION).asString();
            setSchemaUpdateTimeout(context.getPropertyValue(SCHEMA_UPDATE_TIMEOUT).asInteger());

            if (isCloud) {
                client = createCloudClient(connectionString, collection);
            } else {
                client = createHttpClient(connectionString, collection);
            }

            // setup a thread pool of solr updaters
            int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();
            int numConcurrentRequests = context.getPropertyValue(CONCURRENT_REQUESTS).asInteger();
            long flushInterval = context.getPropertyValue(FLUSH_INTERVAL).asLong();
            updaters = new ArrayList<>(numConcurrentRequests);
            for (int i = 0; i < numConcurrentRequests; i++) {
                SolrUpdater updater = new SolrUpdater(client, queue, batchSize, flushInterval);
                new Thread(updater).start();
                updaters.add(updater);
            }

            return client;

        } catch (Exception ex) {
            logger.error(ex.toString());
        }

        return null;
    }

    protected SolrClient getClient() {
        return solrClient;
    }
    protected void setClient(SolrClient client) {
        solrClient = client;
    }

    public String getSolrVersion() {
        return getClient().getClass().getPackage().getImplementationVersion();
    }

    public String getUniqueKey(String collectionName) throws IOException, SolrServerException {
        SchemaRequest.UniqueKey keyRequest = new SchemaRequest.UniqueKey();
        SchemaResponse.UniqueKeyResponse keyResponse = keyRequest.process(getClient(), collectionName);

        return keyResponse.getUniqueKey();
    }

    public void createCollection(String name) throws DatastoreClientServiceException {
        createCollection(name, 1, 0);
    }

    protected void createCloudCollection(String name, int numShards) throws IOException, SolrServerException {
        CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create();
        createRequest.setCollectionName(name);
        createRequest.setNumShards(numShards);

        createRequest.process(getClient());
    }

    protected void createCore(String name) throws IOException, SolrServerException {
        CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
        createRequest.setCoreName(name);
        createRequest.setConfigSet("basic_configs");

        createRequest.process(getClient());
    }

    @Override
    public void createCollection(String name, int numShards, int replicationFactor) throws DatastoreClientServiceException {
        if (existsCollection(name)) {
            return;
        }

        try {
            if (isCloud()) {
                createCloudCollection(name, numShards);
            } else {
                createCore(name);
            }
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    protected void dropCloudCollection(String name) throws IOException, SolrServerException {
        CollectionAdminRequest.Delete deleteRequest = new CollectionAdminRequest.Delete();
        deleteRequest.setCollectionName(name);

        deleteRequest.process(getClient());
    }

    protected void dropCore(String name) throws IOException, SolrServerException {
        CoreAdminRequest.Unload unloadRequest = new CoreAdminRequest.Unload(true);
        unloadRequest.setCoreName(name);
        unloadRequest.setDeleteDataDir(true);
        unloadRequest.setDeleteInstanceDir(true);
        unloadRequest.setDeleteIndex(true);

        unloadRequest.process(getClient());
    }

    @Override
    public void dropCollection(String name)throws DatastoreClientServiceException {
        if (!existsCollection(name)) {
            return;
        }

        try {
            if (isCloud()) {
                dropCloudCollection(name);
            } else {
                dropCore(name);
            }
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }

    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        try {
            SolrQuery q = new SolrQuery("*:*");
            q.setRows(0);  // don't actually request any data

            return getClient().query(name, q).getResults().getNumFound();
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    protected boolean existsCloudCollection(String name) throws  IOException, SolrServerException {
        CollectionAdminRequest.List listRequest = new CollectionAdminRequest.List();
        CollectionAdminResponse response = listRequest.process(getClient(), name);
        if (response.getErrorMessages() != null) {
            throw new DatastoreClientServiceException("Unable to fetch collection list");
        }

        return ((ArrayList) response.getResponse().get("collections")).contains(name);
    }

    protected boolean existsCore(String name) throws IOException, SolrServerException {
        CoreAdminResponse response = CoreAdminRequest.getStatus(name, getClient());

        return response.getCoreStatus(name).size() > 1;
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        try
        {
            if (isCloud()) {
                return existsCloudCollection(name);
            } else {
                return existsCore(name);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    protected void refreshCloudCollection(String name) throws IOException, SolrServerException{
        CollectionAdminRequest.Reload reloadRequest = new CollectionAdminRequest.Reload();

        reloadRequest.process(getClient(), name);
    }

    protected void refreshCore(String name) throws IOException, SolrServerException{
        CoreAdminRequest.reloadCore(name, getClient());
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        if (!existsCollection(name)) {
            return;
        }

        try {
            if (isCloud()) {
                refreshCloudCollection(name);
            } else {
                refreshCore(name);
            }
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRows(1000);
        solrQuery.setQuery("*:*");
        solrQuery.addSort("id", SolrQuery.ORDER.asc);  // Pay attention to this line
        String cursorMark = CursorMarkParams.CURSOR_MARK_START;
        boolean done = false;
        QueryResponse response;
        try {
            do {
                solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                response = getClient().query(src, solrQuery);
                List<SolrInputDocument> documents = new ArrayList<>();
                for (SolrDocument document: response.getResults()) {
                    SolrInputDocument inputDocument = getConverter().toSolrInputDocument(document);
                    inputDocument.removeField("_version_");
                    documents.add(inputDocument);
                }

                getClient().add(dst, documents);

            } while (cursorMark.equals(response.getNextCursorMark()));

            getClient().commit(dst);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void createAlias(String collection, String alias)throws DatastoreClientServiceException {
        try {
            CollectionAdminRequest.CreateAlias createAlias = new CollectionAdminRequest.CreateAlias();
            createAlias.setAliasedCollections(collection);
            createAlias.setAliasName(alias);

            CollectionAdminResponse response = createAlias.process(getClient());
            response.isSuccess();
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    public List<Map<String, Object>> getMapping(String collectionName) throws IOException, SolrServerException {
        SchemaRequest.Fields request = new SchemaRequest.Fields();
        SchemaResponse.FieldsResponse response = request.process(getClient(), collectionName);

        return response.getFields();
    }

    public boolean removeMapping(String collectionName, List<Map<String, Object>> mapping)
            throws DatastoreClientServiceException {
        Boolean result = true;
        try {
            for (Map<String, Object> field: mapping) {
                SchemaRequest.DeleteField schemaRequest = new SchemaRequest.DeleteField((String) field.get("name"));
                SchemaResponse.UpdateResponse response = schemaRequest.process(getClient(), collectionName);
                result = result && response.getStatus() == 0 && response.getResponse().get("errors") == null;
            }

            getClient().commit(collectionName);
            refreshCollection(collectionName);

            return result;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    public boolean putMapping(String collectionName, List<Map<String, Object>> mapping)
            throws DatastoreClientServiceException {
        Boolean result = true;
        try {
            for (Map<String, Object> field: mapping) {
                SchemaRequest.AddField schemaRequest = new SchemaRequest.AddField(field);

                if (isCloud()) {
                    Set<String> params = new HashSet<>();
                    params.add("updateTimeoutSecs="+getSchemaUpdateTimeout());
                    schemaRequest.setQueryParams(params);
                }

                SchemaResponse.UpdateResponse response = schemaRequest.process(getClient(), collectionName);
                result = result && response.getStatus() == 0 && response.getResponse().get("errors") == null;
            }

            getClient().commit(collectionName);
            refreshCollection(collectionName);

            return result;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString)
            throws DatastoreClientServiceException {

        return false;
    }

    public void bulkFlush(String collectionName) throws DatastoreClientServiceException {
        try {
            getClient().commit(collectionName);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {
        bulkFlush(null);
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        put(collectionName, record, false, false);
    }

    public void put(String collectionName, Record record) throws DatastoreClientServiceException {
        put(collectionName, record, false, true);
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        put(collectionName, record, asynchronous, true);
    }

    public void put(String collectionName, Record record, boolean asynchronous, boolean autoCommit) throws DatastoreClientServiceException {
        try {
            SolrInputDocument document = getConverter().toSolrInputDocument(record, getUniqueKey(collectionName));

            getClient().add(collectionName, document);

            if (autoCommit) {
                getClient().commit(collectionName);
            }
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }



    @Override
    public void waitUntilCollectionReady(String name, long timeoutMilli) throws DatastoreClientServiceException {
        throw new UnsupportedOperationException("not yet supported for SolrClientService");
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        try {
            List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();

            for (MultiGetQueryRecord multiGetQueryRecord : multiGetQueryRecords)
            {
                String index = multiGetQueryRecord.getIndexName();
                String uniqueKeyName = getUniqueKey(index);

                String[] fieldsToInclude = multiGetQueryRecord.getFieldsToInclude();
                Map<String, String[]> params = new HashMap<>();
                if (fieldsToInclude != null && fieldsToInclude.length > 0) {
                    ArrayList<String> fields = new ArrayList<>();
                    fields.addAll(Arrays.asList(fieldsToInclude));
                    if (!fields.contains(uniqueKeyName)) {
                        fields.add(uniqueKeyName);
                    }

                    params.put("fl", fields.toArray(new String[fields.size()]));
                }


                SolrParams solrParams = new ModifiableSolrParams(params);

                SolrDocumentList documents = getClient().getById(index, multiGetQueryRecord.getDocumentIds(), solrParams);

                for (SolrDocument document: documents) {
                    Map<String, Map<String, String>> map = getConverter().toMap(document, uniqueKeyName);
                    Map.Entry<String,Map<String, String>> mapDocument = map.entrySet().iterator().next();

                    multiGetResponseRecords.add(
                            new MultiGetResponseRecord(index, "", mapDocument.getKey(), mapDocument.getValue())
                    );
                }
            }

            return multiGetResponseRecords;
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        return get(collectionName, record.getId());
    }

    public Record get(String collectionName, String id) throws DatastoreClientServiceException {
        try {
            SolrDocument document = getClient().getById(collectionName, id);

            return getConverter().toRecord(document, getUniqueKey(collectionName));
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    public Collection<Record> query(String queryString, String collectionName) {
        try {
            String uniqueKey = getUniqueKey(collectionName);
            SolrQuery query = new SolrQuery();
            query.setQuery(queryString);

            QueryResponse response = getClient().query(collectionName, query);

            Collection<Record> results = new ArrayList<>();
            response.getResults().forEach(document -> {
                results.add(getConverter().toRecord(document, uniqueKey));
            });

        } catch (SolrServerException | IOException e) {
            logger.error(e.toString());
            throw new DatastoreClientServiceException(e);
        }

        return null;
    }

    @Override
    public Collection<Record> query(String queryString) {
        return query(queryString, null);
    }

    public long queryCount(String queryString, String collectionName) {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery(queryString);

            QueryResponse response = getClient().query(collectionName, query);

            return response.getResults().getNumFound();

        } catch (SolrServerException | IOException e) {
            logger.error(e.toString());
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public long queryCount(String queryString) {
        return queryCount(queryString, null);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        try {
            getClient().deleteById(collectionName, record.getId());

        } catch (SolrServerException | IOException e) {
            logger.error(e.toString());
            throw new DatastoreClientServiceException(e);
        }
    }
}
