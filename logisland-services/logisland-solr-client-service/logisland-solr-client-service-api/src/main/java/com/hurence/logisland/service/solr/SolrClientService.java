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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.*;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Tags({ "solr", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Solr 5.5.5.")
abstract public class SolrClientService extends AbstractControllerService implements DatastoreClientService {

    protected volatile SolrClient solrClient;
    protected Boolean isCloud;
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

    public Boolean isCloud(Boolean isCloud) {
        this.isCloud = isCloud;

        return this.isCloud;
    }

    public Boolean isCloud() {
        return isCloud;
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

        return Collections.unmodifiableList(props);
    }

    @Override
    @OnEnabled
    public void init(ControllerServiceInitializationContext context) throws InitializationException  {
        synchronized(this) {
            try {
                createSolrClient(context);
            }catch (Exception e){
                throw new InitializationException(e);
            }
        }
    }

    abstract protected void createCloudClient(String connectionString, String collection);
    abstract protected void createHttpClient(String connectionString, String collection);


    /**
     * Instantiate ElasticSearch Client. This chould be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    protected void createSolrClient(ControllerServiceInitializationContext context) throws ProcessException {
        if (solrClient != null) {
            return;
        }
        try {

            // create a solr client
            final boolean isCloud = context.getPropertyValue(SOLR_CLOUD).asBoolean();
            final String connectionString = context.getPropertyValue(SOLR_CONNECTION_STRING).asString();
            final String collection = context.getPropertyValue(SOLR_COLLECTION).asString();


            if (isCloud) {
                createCloudClient(connectionString, collection);
            } else {
                createHttpClient(connectionString, collection);
            }


            // setup a thread pool of solr updaters
            int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();
            int numConcurrentRequests = context.getPropertyValue(CONCURRENT_REQUESTS).asInteger();
            long flushInterval = context.getPropertyValue(FLUSH_INTERVAL).asLong();
            updaters = new ArrayList<>(numConcurrentRequests);
            for (int i = 0; i < numConcurrentRequests; i++) {
                SolrUpdater updater = new SolrUpdater(solrClient, queue, batchSize, flushInterval);
                new Thread(updater).start();
                updaters.add(updater);
            }

        } catch (Exception ex) {
            logger.error(ex.toString());
        }
    }

    protected SolrClient getClient() {
        return solrClient;
    }
    protected void setClient(SolrClient client) {
        solrClient = client;
    }

    public void createCollection(String name) throws DatastoreClientServiceException {
        createCollection(name, 0, 0);
    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {
        try {
            if (!existsCollection(name))
            {
                CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();

                createRequest.setCoreName(name);
                createRequest.setConfigSet("basic_configs");
                createRequest.process(getClient());
            }
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void dropCollection(String name)throws DatastoreClientServiceException {
        try {
            if (existsCollection(name))
            {
                CoreAdminRequest.Unload unloadRequest = new CoreAdminRequest.Unload(true);
                unloadRequest.setCoreName(name);
                unloadRequest.setDeleteDataDir(true);
                unloadRequest.setDeleteInstanceDir(true);
                unloadRequest.setDeleteIndex(true);
                CoreAdminResponse response = unloadRequest.process(getClient());
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

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        try
        {
            // Request core list
            CoreAdminRequest request = new CoreAdminRequest();
            request.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            CoreAdminResponse cores = request.process(solrClient);

            // List of the cores
            List<String> coreList = new ArrayList<String>();
            for (int i = 0; i < cores.getCoreStatus().size(); i++) {
                coreList.add(cores.getCoreStatus().getName(i));
            }
            CoreAdminResponse aResponse = CoreAdminRequest.getStatus(name, getClient());

            return aResponse.getCoreStatus(name).size() > 1;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return false;
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        try {
            CoreAdminResponse aResponse = CoreAdminRequest.getStatus(name, getClient());

            if (aResponse.getCoreStatus(name).size() > 0)
            {
                CoreAdminRequest.reloadCore(name, getClient());
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
                    // TODO - Use Backup/Restore in Solr 6 ?
                    SolrInputDocument inputDocument = ClientUtils.toSolrInputDocument(document);
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
            createAlias.process(getClient());
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    public boolean putMapping(String collectionName, List<Map<String, Object>> mapping)
            throws DatastoreClientServiceException {
        Boolean result = true;
        try {
            for (Map<String, Object> field: mapping) {
                SchemaRequest.AddField schemaRequest = new SchemaRequest.AddField(field);
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
        try {
            getClient().commit();
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        try {
            _put(collectionName, record);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }

    }

    public String getUniqueKey(String collectionName) throws IOException, SolrServerException {
        SchemaRequest.UniqueKey keyRequest = new SchemaRequest.UniqueKey();
        SchemaResponse.UniqueKeyResponse keyResponse = keyRequest.process(getClient(), collectionName);

        return keyResponse.getUniqueKey();
    }

    public void put(String collectionName, Record record) throws DatastoreClientServiceException {
        put(collectionName, record, false);
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        try {
            _put(collectionName, record);

            getClient().commit(collectionName);
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    protected void _put(String collectionName, Record record) throws IOException, SolrServerException {
        SolrInputDocument document = new SolrInputDocument();

        document.addField(getUniqueKey(collectionName), record.getId());
        for (Field field : record.getAllFields()) {
            if (field.isReserved()) {
                continue;
            }

            document.addField(field.getName(), field.getRawValue());
        }

        getClient().add(collectionName, document);
    }

    /* ********************************************************************
     * Get handling section
     * ********************************************************************/

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        try {
            List<MultiGetResponseRecord> multiGetResponseRecords = new ArrayList<>();
            Set<String> documentIds = new HashSet<>();

            for (MultiGetQueryRecord multiGetQueryRecord : multiGetQueryRecords)
            {
                String index = multiGetQueryRecord.getIndexName();
                String type = multiGetQueryRecord.getTypeName();
                String[] fieldsToInclude = multiGetQueryRecord.getFieldsToInclude();
                String[] fieldsToExclude = multiGetQueryRecord.getFieldsToExclude();
//            if ((fieldsToInclude != null && fieldsToInclude.length > 0) || (fieldsToExclude != null && fieldsToExclude.length > 0)) {
//                for (String documentId : documentIds) {
//                    MultiGetRequest.Item item = new MultiGetRequest.Item(index, type, documentId);
//                    item.fetchSourceContext(new FetchSourceContext(true, fieldsToInclude, fieldsToExclude));
//                    multiGetRequestBuilder.add(item);
//                }
//            } else {
//                multiGetRequestBuilder.add(index, type, documentIds);
//            }
                SolrDocumentList documents = getClient().getById(index, multiGetQueryRecord.getDocumentIds());
                String uniqueKeyName = getUniqueKey(index);
                String uniqueKeyValue = null;
                Map<String,String> retrievedFields = new HashMap<>();
                for (SolrDocument document: documents) {
                    for (Map.Entry<String, Object> entry: document.entrySet()) {
                        String name = entry.getKey();
                        Object value = entry.getValue();
                        if (name.startsWith("_")) {
                            // reserved keyword
                            continue;
                        }
                        if (name.equals(uniqueKeyName)) {
                            uniqueKeyValue = (String) value;
                            continue;
                        }
                        // TODO - Discover Type
                        retrievedFields.put(name, value.toString());
                    }

                    multiGetResponseRecords.add(
                            new MultiGetResponseRecord(index, "", uniqueKeyValue, retrievedFields)
                    );
                }
            }


//        MultiGetResponse multiGetItemResponses = null;
//        try {
//            multiGetItemResponses = multiGetRequestBuilder.get();
//        } catch (ActionRequestValidationException e) {
//            getLogger().error("MultiGet query failed : {}", new Object[]{e.getMessage()});
//        }
//
//        if (multiGetItemResponses != null) {
//            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
//                GetResponse response = itemResponse.getResponse();
//                if (response != null && response.isExists()) {
//                    Map<String,Object> responseMap = response.getSourceAsMap();
//                    Map<String,String> retrievedFields = new HashMap<>();
//                    responseMap.forEach((k,v) -> {if (v!=null) retrievedFields.put(k, v.toString());});
//                    multiGetResponseRecords.add(new MultiGetResponseRecord(response.getIndex(), response.getType(), response.getId(), retrievedFields));
//                }
//            }
//        }

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
            Record record = new StandardRecord();
            String uniqueKey = getUniqueKey(collectionName);
            for (Map.Entry<String, Object> entry: document.entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                if (name.startsWith("_")) {
                    // reserved keyword
                    continue;
                }
                if (name.equals(uniqueKey)) {
                    record.setId((String) value);
                    continue;
                }
                // TODO - Discover Type
                record.setField(name, FieldType.STRING, value);
            }

            return record;
        } catch (Exception e) {
            throw new DatastoreClientServiceException(e);
        }
    }

    @Override
    public Collection<Record> query(String queryString) {
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery(queryString);

            QueryResponse response = getClient().query(query);

            //response.getResults().forEach(doc -> doc.);

        } catch (SolrServerException | IOException e) {
            logger.error(e.toString());
            throw new DatastoreClientServiceException(e);
        }

        return null;
    }

    public long queryCount(String collectionName, String queryString) {
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
        try {
            SolrQuery query = new SolrQuery();
            query.setQuery(queryString);

            QueryResponse response = getClient().query(query);

            return response.getResults().getNumFound();

        } catch (SolrServerException | IOException e) {
            logger.error(e.toString());
            throw new DatastoreClientServiceException(e);
        }
    }
}
