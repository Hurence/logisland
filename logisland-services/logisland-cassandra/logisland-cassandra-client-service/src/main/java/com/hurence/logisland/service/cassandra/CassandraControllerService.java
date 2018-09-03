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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnStopped;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.NotImplementedException;
import com.hurence.logisland.service.cassandra.RecordConverter.CassandraType;

import java.util.*;
import java.util.concurrent.*;

@Tags({"cassandra", "service"})
@CapabilityDescription(
        "Provides a controller service that wraps most of the functionality of the Cassandra driver."
)
public class CassandraControllerService extends AbstractControllerService implements CassandraClientService {

    private Cluster cluster;
    private Session session;
    private String keyspace;
    private String table;
    Map<String, CassandraType> fieldsToType = new HashMap<String, CassandraType>();
    List<String> primaryFields = new ArrayList<String>();
    private boolean createSchema = true;
    private boolean ssl = false;
    private boolean credentials = false;
    private CassandraUpdater updater;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private long flushInterval;
    final BlockingQueue<Record> queue = new ArrayBlockingQueue<>(100000);
    volatile boolean stillSomeRecords = false; // Unit tests only code

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

    protected static final PropertyDescriptor KEYSPACE = new PropertyDescriptor.Builder()
            .name("cassandra.keyspace")
            .displayName("Cassandra keyspace name")
            .description("The name of the keyspace to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("cassandra.table")
            .displayName("Cassandra table name")
            .description("The name of the table to use in the keyspace")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TABLE_FIELDS = new PropertyDescriptor.Builder()
            .name("cassandra.table.fields")
            .displayName("Cassandra table fields and types")
            .description("Tne names of the table fields and their cassandra types. For a bulkput, each field must be an " +
                    "existing incoming logisland record field. The format of this property is: <record_field1>:<cassandra_type1>[,<record_fieldN>:<cassandra_typeN>]. " +
                    "Example: record_id:uuid,record_time:timestamp,intValue,textValue.")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    protected static final PropertyDescriptor TABLE_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .name("cassandra.table.primary_key")
            .displayName("Cassandra table primary key")
            .description("Tne ordered names of the fields forming the primary key in the table to create. " +
                    "The format of this property is: primaryKeyField[,<nextPrimaryKeyFieldN>]. " +
                    "Example: record_id,intValue")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CREATE_SCHEMA = new PropertyDescriptor.Builder()
            .name("cassandra.schema.create")
            .displayName("Create or not the cassandra schema if it does not exist.")
            .description("If this property is true, then if they do not exist, the keyspace and the table with its defined fields and primary key will be created at initialization time." +
                    "Otherwise, all these elements are expected to already exist in the cassandra cluster")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
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
        descriptors.add(KEYSPACE);
        descriptors.add(TABLE);
        descriptors.add(TABLE_FIELDS);
        descriptors.add(TABLE_PRIMARY_KEY);
        descriptors.add(CREATE_SCHEMA);
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

        Cluster.Builder builder = Cluster.builder();

        // Hosts
        String[] cassandraHosts = context.getPropertyValue(HOSTS).asString().split(" ,");
        List<String> hosts = new ArrayList<String>();
        for (String host : cassandraHosts)
        {
            builder.addContactPoint(host);
            hosts.add(host);
        }

        // Use SSL?
        if (context.getPropertyValue(WITH_SSL).isSet())
            ssl = context.getPropertyValue(WITH_SSL).asBoolean();
        if (ssl)
        {
            builder.withSSL();
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
            builder.withCredentials(userName, userPassword);
        }

        // Port
        int port = context.getPropertyValue(PORT).asInteger();
        builder.withPort(port);

        String credDetails = "credentials=no";
        if (credentials)
        {
            credDetails = "credentials=" + userName;
        }
        getLogger().info("Establishing Cassandra connection to hosts " + hosts + " on port " + port
                + " ssl=" + ssl + " " + credDetails);

        // Connect
        cluster = builder.build();
        session = cluster.connect();

        getLogger().info("Connected to Cassandra");

        /**
         * Get other configuration properties
         */

        keyspace = context.getPropertyValue(KEYSPACE).asString();
        table = context.getPropertyValue(TABLE).asString();
        getTableFields(context.getPropertyValue(TABLE_FIELDS).asString());
        getTablePrimaryKey(context.getPropertyValue(TABLE_PRIMARY_KEY).asString());

        if (context.getPropertyValue(CREATE_SCHEMA).isSet())
            createSchema = context.getPropertyValue(CREATE_SCHEMA).asBoolean();

        if (createSchema)
        {
            createSchema();
        }

        startUpdater(context);
    }

    /**
     * Creates the schema that is:
     * - the keyspace if it does not exist
     * - the table if it does not exist
     */
    private void createSchema() {

        getLogger().info("Creating cassandra schema (keyspace and table) if it does not exist");

        /**
         * Create keyspace
         */

        String statement = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};";
        getLogger().debug(statement);
        session.execute(statement);

        /**
         * Create table
         *
         * CREATE TABLE IF NOT EXISTS keyspace.table (
         *     foo uuid,
         *     bar int,
         *     last text,
         *     PRIMARY KEY (foo, bar)
         * );
         */

        StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS " + keyspace + "." + table + " (\n");
        fieldsToType.forEach(
                (field, type) -> { sb.append(field).append(" ").append(type.getValue()).append(",\n"); }
        );
        sb.append("PRIMARY KEY (");
        boolean first = true;
        for (String field : primaryFields) {
                if (first) {
                    first = false;
                } else
                {
                    sb.append(", ");
                }
                sb.append(field);
        }
        sb.append(")\n);");
        statement = sb.toString();
        getLogger().info(statement);
        session.execute(statement);
    }

    /**
     * Parses value of TABLE_FIELDS
     * @param value
     */
    private void getTableFields(String value) throws InitializationException {

        String[] fieldAndTypes = value.split(",");
        for (String fieldAndTypeString : fieldAndTypes)
        {
            String[] fieldAndType = fieldAndTypeString.trim().split(":");

            if (fieldAndType.length != 2 )
            {
                throw new InitializationException("missing ':' character separator in <" + fieldAndTypeString + ">");
            }

            String field = fieldAndType[0].trim();
            if (field.length() == 0)
            {
                throw new InitializationException("missing field name in <" + fieldAndTypeString + ">");
            }
            String type = fieldAndType[1];
            if (type.length() == 0)
            {
                throw new InitializationException("missing type name in <" + fieldAndTypeString + ">");
            }

            CassandraType cassandraType = null;
            try {
                cassandraType = CassandraType.fromValue(type);
            } catch (Exception e) {
                throw new InitializationException(e);
            }
            fieldsToType.put(field, cassandraType);
        }
    }

    /**
     * Parses value of TABLE_PRIMARY_KEY
     * @param value
     */
    private void getTablePrimaryKey(String value) throws InitializationException {

        String[] fields = value.split(",");
        for (String field : fields)
        {
            field = field.trim();

            if (field.length() == 0)
            {
                throw new InitializationException("Empty field in <" + value + ">");
            }

            // Check that the field is in the declared table fields
            CassandraType cassandraType = fieldsToType.get(field);
            if (cassandraType == null)
            {
                throw new InitializationException("Undefined field <" + field + "> in table fields list");
            }

            primaryFields.add(field);
        }
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
        if (cluster != null) {
            cluster.close();
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
        updater = new CassandraUpdater(cluster, session, keyspace, table, queue , batchSize, this, flushInterval);

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

    /**
     * Unit tests only code
     */
    public void waitForFlush()
    {
        // First wait for empty queue
        while (!queue.isEmpty())
        {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                getLogger().error("Interrupted while waiting for cassandra updater flush [step 1]");
            }
        }

        // Then wait for all records sent to cassandra
        while (this.stillSomeRecords)
        {
            try {
                Thread.sleep(1000);
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
        if (record != null) {
            stillSomeRecords = true;
            queue.add(record);
        } else
            getLogger().debug("Trying to add null record in the queue");
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
