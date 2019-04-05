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
package com.hurence.logisland.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnStopped;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.NotImplementedException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

@Tags({"cassandra", "service"})
@CapabilityDescription(
        "Provides a controller service that for the moment only allows to bulkput records into cassandra."
)
public class CassandraControllerService extends AbstractControllerService implements CassandraClientService {

    private CqlSession session;
    private boolean ssl = false;
    private boolean credentials = false;
    private CassandraUpdater updater;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long flushInterval;
    final BlockingQueue<RecordToIndex> queue = new ArrayBlockingQueue<>(100000);
    volatile boolean stillSomeRecords = false; // Unit tests only code

    /**
     * Holds a record to index and its meta data
     */
    static class RecordToIndex
    {
        // Destination keyspace.table
        private String collectionName = null;
        // Record to index
        private Record record;

        public RecordToIndex(String collectionName, Record record)
        {
            this.collectionName = collectionName;
            this.record = record;
        }

        public String getCollectionName() {
            return collectionName;
        }

        public Record getRecord() {
            return record;
        }
    }

    protected static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("cassandra.hosts")
            .displayName("Cassandra hosts")
            .description("Cassandra cluster hosts as a comma separated value list")
            .required(true)
            .addValidator(Validation.HOSTS_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("cassandra.port")
            .displayName("Cassandra port")
            .description("Cassandra cluster port")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    protected static final PropertyDescriptor WITH_SSL = new PropertyDescriptor.Builder()
            .name("cassandra.with-ssl")
            .displayName("Use SSL.")
            .description("If this property is true, use SSL. Default is no SSL (false).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor WITH_CREDENTIALS = new PropertyDescriptor.Builder()
            .name("cassandra.with-credentials")
            .displayName("Use credentials.")
            .description("If this property is true, use credentials. Default is no credentials (false).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREDENTIALS_USER = new PropertyDescriptor.Builder()
            .name("cassandra.credentials.user")
            .displayName("User name.")
            .description("The user name to use for authentication. " + WITH_CREDENTIALS.getName() + " must be true for that property to be used.")
            .required(false)
            .build();

    protected static final PropertyDescriptor CREDENTIALS_PASSWORD = new PropertyDescriptor.Builder()
            .name("cassandra.credentials.password")
            .displayName("User password.")
            .description("The user password to use for authentication. " + WITH_CREDENTIALS.getName() + " must be true for that property to be used.")
            .required(false)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();

        descriptors.add(HOSTS);
        descriptors.add(PORT);
        descriptors.add(WITH_SSL);
        descriptors.add(WITH_CREDENTIALS);
        descriptors.add(CREDENTIALS_USER);
        descriptors.add(CREDENTIALS_PASSWORD);
        descriptors.add(BATCH_SIZE);
        descriptors.add(BULK_SIZE);
        descriptors.add(FLUSH_INTERVAL);
        return descriptors;
    }

    @Override
    public void init(ControllerServiceInitializationContext context) throws InitializationException {

        /**
         * Get config and establish connection to cassandra
         */
        super.init(context);
        CqlSessionBuilder builder = CqlSession.builder();

        // Hosts
        String[] cassandraHosts = context.getPropertyValue(HOSTS).asString().split(" ,");
        // Port
        int port = context.getPropertyValue(PORT).asInteger();

        List<String> hosts = new ArrayList<String>();
        for (String host : cassandraHosts)
        {
            builder.addContactPoint(new InetSocketAddress(host, port));
            hosts.add(host);
        }

        // Use SSL?
        if (context.getPropertyValue(WITH_SSL).isSet())
            ssl = context.getPropertyValue(WITH_SSL).asBoolean();
        if (ssl)
        {
//            TODO SSL AND CREDENTIAL
//            builder.withSSL();
        }

        // Use credentials?
        if (context.getPropertyValue(WITH_CREDENTIALS).isSet())
            credentials = context.getPropertyValue(WITH_CREDENTIALS).asBoolean();
        String userName = "";
        if (credentials)
        {
            // User name
            if (!context.getPropertyValue(CREDENTIALS_USER).isSet())
            {
                throw new InitializationException("Credentials are enabled but user name is null");
            }
            userName = context.getPropertyValue(CREDENTIALS_USER).asString();
            if (userName.length() == 0)
            {
                throw new InitializationException("Credentials are enabled but user name is empty");
            }

            // User password
            if (!context.getPropertyValue(CREDENTIALS_PASSWORD).isSet())
            {
                throw new InitializationException("Credentials are enabled but user password is null");
            }
            String userPassword = context.getPropertyValue(CREDENTIALS_PASSWORD).asString();
            if (userPassword.length() == 0)
            {
                throw new InitializationException("Credentials are enabled but user password is empty");
            }
//            builder.withCredentials(userName, userPassword);
        }
//        RemoteEndpointAwareSSLOptions
//        DriverContext ct = new DriverOptionConfigBuilder().with
//        DriverConfigLoader ff = DefaultDriverConfigLoaderBuilder.with
//        builder.withConfigLoader();
        //See configuration

        String credDetails = "credentials=no";
        if (credentials)
        {
            credDetails = "credentials=" + userName;
        }
        getLogger().info("Establishing Cassandra connection to hosts " + hosts + " on port " + port
                + " ssl=" + ssl + " " + credDetails);

        // Connect
        session = builder.build();

        getLogger().info("Connected to Cassandra");

        startUpdater(context);
    }

    // Note: we use the @OnDisabled facility here so that unit test can call proper disconnection with
    // runner.disableControllerService(service); runner has no stopControllerService(service)
    // This service does not however currently supports disable/enable out of unit test
    @OnDisabled
    @OnStopped
    public final void stop() {

        stopUpdater();

        if (session != null) {
            session.close();
        }
    }

    /**
     * Starts the updaters
     */
    private void startUpdater(ControllerServiceInitializationContext context)
    {

        /**
         * Prepare the update
         */

        // setup a thread pool of cassandra updaters
        int batchSize = context.getPropertyValue(BATCH_SIZE).asInteger();
        flushInterval = context.getPropertyValue(FLUSH_INTERVAL).asLong();
        updater = new CassandraUpdater(session, queue , batchSize, this, flushInterval);

        executorService.execute(updater);
    }

    /**
     * Stops the updater
     */
    private void stopUpdater()
    {
        updater.stop();

        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            getLogger().error("Timeout waiting for cassandra updater to terminate");
        }

        updater = null;
    }

    @Override
    public void createCollection(String name, int partitionsCount, int replicationFactor) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public void dropCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public long countCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public boolean existsCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public void refreshCollection(String name) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public void copyCollection(String reindexScrollTimeout, String src, String dst) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public void createAlias(String collection, String alias) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public boolean putMapping(String indexName, String doctype, String mappingAsJsonString) throws DatastoreClientServiceException {
        getLogger().warn("putMapping not implemented for Cassandra");
        return false;
    }

    // Special collection name used in unit test to know when the last record of the test is treated
    public static final String END_OF_TEST = "endoftest";

    /**
     * Unit tests only code
     */
    public void waitForFlush()
    {
        // Then wait for all records sent to cassandra
        while (this.stillSomeRecords)
        {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                getLogger().error("Interrupted while waiting for cassandra updater flush [step 2]");
            }
        }
    }

    @Override
    public void bulkFlush() throws DatastoreClientServiceException {

        // TODO: current bulkPut processor implementation systematically calls bulkFlush. I think it should not otherwise
        // what's the point in having a dedicated thread for insertion (updater thread)?
        // If we put some mechanism to wait for flush here, the perf will be impacted. So for test purpose only,
        // I set the flush mechanism in waitForFlush
    }

    @Override
    public void bulkPut(String collectionName, Record record) throws DatastoreClientServiceException {
        if ( (collectionName != null) && (record != null) ) {
            stillSomeRecords = true;
            queue.add(new RecordToIndex(collectionName.toLowerCase(), record));
        } else {
            if ( (record != null) && (collectionName == null) )
            {
                record.addError(ProcessError.UNKNOWN_ERROR.toString(),
                        "Trying to bulkput to Cassandra with a null collection name");
            }

            if (record == null)
                getLogger().error("Trying to add a null record in the queue");
        }
    }

    @Override
    public void put(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        // For time being, support it through bulkPut
        bulkPut(collectionName, record);
    }

    @Override
    public void remove(String collectionName, Record record, boolean asynchronous) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public List<MultiGetResponseRecord> multiGet(List<MultiGetQueryRecord> multiGetQueryRecords) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public Record get(String collectionName, Record record) throws DatastoreClientServiceException {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public Collection<Record> query(String query) {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }

    @Override
    public long queryCount(String query) {
        throw new NotImplementedException("Not yet supported for Cassandra");
    }
}
