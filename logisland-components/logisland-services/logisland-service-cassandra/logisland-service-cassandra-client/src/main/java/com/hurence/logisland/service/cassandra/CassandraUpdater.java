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
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.cassandra.CassandraControllerService.RecordToIndex;
import com.hurence.logisland.service.cassandra.RecordConverter.CassandraType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hurence.logisland.service.cassandra.CassandraControllerService.END_OF_TEST;

/**
 * This is a Runnable class used to buffer record to bulk put into Cassandra
 */
public class CassandraUpdater implements Runnable {

    private CassandraControllerService service;
    private final BlockingQueue<RecordToIndex> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0L;
    volatile boolean stop = false;

    private CqlSession session;

    private static volatile int threadCount = 0;

    private Logger logger = LoggerFactory.getLogger(CassandraUpdater.class.getName() + threadCount);

    // keyspace.table -> TableData to use for insertion into the table
    private Map<String, TableData> tablesData = new HashMap<String, TableData>();

    /**
     * Holds any info/object about a known table
     */
    private static class TableData {

        // Prepared statement to use for inserting data into the table
        private BoundStatement boundStatement = null;
        // Ordered list of fields of the table and their types
        private LinkedHashMap<String, CassandraType> orderedFields;

        public TableData(BoundStatement boundStatement, LinkedHashMap<String, CassandraType> orderedFields)
        {
            this.boundStatement = boundStatement;
            this.orderedFields = orderedFields;
        }

        public BoundStatement getBoundStatement() {
            return boundStatement;
        }

        public LinkedHashMap<String, CassandraType> getOrderedFields() {
            return orderedFields;
        }
    }

    public CassandraUpdater(CqlSession session, BlockingQueue<RecordToIndex> records, int batchSize,
                            CassandraControllerService service, long flushInterval) {
        this.session = session;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.service = service;
        threadCount++;
    }

    /**
     * Prepare once for all a bound statement that will be always used for a table: this is faster than constructing and using a
     * specific string each time.
     */
    private BoundStatement prepareStatement(String collectionName, LinkedHashMap<String, CassandraType> tableSchema) {

        /**
         * INSERT INTO keyspace.table (field1, field2) VALUES (?, ?)
         */
        StringBuffer sb = new StringBuffer("INSERT INTO " + collectionName + " (");
        StringBuffer questionMarksSb = new StringBuffer();
        boolean first = true;
        for (String field : tableSchema.keySet()) {
            if (first) {
                first = false;
            } else
            {
                sb.append(", ");
                questionMarksSb.append(", ");
            }
            sb.append(field);
            questionMarksSb.append("?");
        }
        sb.append(") VALUES (").append(questionMarksSb).append(");");
        String statementString = sb.toString();
        PreparedStatement statement = session.prepare(statementString);
        return statement.bind();
//        return new BoundStatement(statement);
    }

    /**
     * Parse cassandra schema to find the fields and types of a particular collection if it exists
     * @param metaData Cassandra Metadata to parse TODO
     * @param collectionName Name of the table to find schema for (with the form: keyspace.table)
     * @return The found schema for the passed collection (aka table) or null if one could not find schema for any reason
     * (table does not exist, error while parsing..;)
     */
    private LinkedHashMap<String, CassandraType> parseSchemaForCollection(Metadata metaData, String collectionName)
    {
//        LinkedHashMap<String, CassandraType> mapping = new LinkedHashMap<String, CassandraType>();
        String keySpace = null;
        String tableName = null;
        String[] tokens = collectionName.split("\\.");
        if (tokens.length != 2) {
            logger.error("Error finding keyspace and table name from collection name '{}'", collectionName);
            return null;
        }
        logger.info("keyspace is {}, table name is {}", keySpace, tableName);
        keySpace = tokens[0].trim();
        tableName = tokens[1].trim();
        Optional<KeyspaceMetadata> keySpaceMeta = metaData.getKeyspace(keySpace);
        Optional<TableMetadata> tableMeta = keySpaceMeta.get().getTable(tableName);

        return parseSchemaForCollection(tableMeta.get().describe(true), collectionName);

//        tableMeta.get().getColumns().entrySet().forEach(c -> {
//            mapping.put(c.getKey().asInternal(), c.getValue().getType().toString());
//        });
//        session.getContext().getSslEngineFactory()
//        metaData.
//        return null;
    }


    /**
     * Parse cassandra schema to find the fields and types of a particular collection if it exists
     * @param schema Cassandra schema to parse
     * @param collectionName Name of the table to find schema for (with the form: keyspace.table)
     * @return The found schema for the passed collection (aka table) or null if one could not find schema for any reason
     * (table does not exist, error while parsing..;)
     */
    private LinkedHashMap<String, CassandraType> parseSchemaForCollection(String schema, String collectionName)
    {
        LinkedHashMap<String, CassandraType> mapping = new LinkedHashMap<String, CassandraType>();

        BufferedReader reader = new BufferedReader(new StringReader(schema));

        /**
         * We look for sequences like this. from "CREATE TABLE" with the right collection name to the
         * ending ") WITH" statement. The "PRIMARY KEY" statement may be on a single field or at the end if this is a
         * composite key. So if we see the "PRIMARY KEY" statement at the end, this is the end of the parsing.
         * Examples for both cases:
         *
         * Unique primary key example:
         *
         * CREATE TABLE keyspace.table (
         *     field1 uuid PRIMARY KEY,
         *     field2 int,
         *     field3 text
         * ) WITH bloom_filter_fp_chance = 0.01 ...
         *
         * Composite primary key example:
         *
         * CREATE TABLE keyspace.table (
         *     field1 uuid,
         *     field2 int,
         *     field3 text,
         *     PRIMARY KEY (field1, field2)
         * ) WITH CLUSTERING ORDER BY (field2 ASC) ...
         */

        final String CREATE_TABLE = "CREATE TABLE";
        final String WITH = ") WITH";
        final String PRIMARY_KEY = "PRIMARY KEY";
        final String BLANK_REGEX = "\\s+"; // Regex for any white space (space, tab...)
        boolean startFound = false;
        try {
            String line = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim(); // Remove any beginning or trailing space, tab...

                // Is it the beginning of a table description?
                if (line.startsWith(CREATE_TABLE))
                {
                    // Get the table collection name (keyspace.table)
                    line = line.substring(CREATE_TABLE.length()).trim(); // Remove CREATE TABLE
                    String[] tokens = line.split(BLANK_REGEX);
                    String table = tokens[0]; // Get first token
                    if (table.equals(collectionName))
                    {
                        // This is the first line for the table we are interested in
                        startFound = true;
                    }
                } else if (startFound)
                {
                    // Is it the end of the part we are interested in for the table?
                    if (line.startsWith(WITH) || line.startsWith(PRIMARY_KEY))
                    {
                        // End of the field list
                        break;
                    }

                    // This is a field declaration line we have to take into account

                    // Handle the case where there is a unique primary key. In this case just take the first part of the line
                    int primaryIndex = line.indexOf(PRIMARY_KEY);
                    if (primaryIndex != -1)
                    {
                        // Remove the end
                        line = line.substring(0, primaryIndex).trim();
                    }

                    // Here we get a string with the form "<field> <type>," or "<field> <type> PRIMARY KEY,"
                    String[] tokens = line.split(BLANK_REGEX);
                    String field = tokens[0];
                    String type = tokens[1];
                    type = type.split(",")[0]; // Remove potential final comma character
                    CassandraType cassandraType = null;
                    try {
                        cassandraType = CassandraType.fromValue(type);
                    } catch (Exception e) {
                        logger.error("Error fetching schema for " + collectionName + ": " + e.getMessage());
                        return null;
                    }
                    mapping.put(field, cassandraType);
                }
            }

            if (mapping.size() != 0)
            {
                return mapping;
            } else
            {
                return null;
            }
        } catch (IOException e)
        {
            logger.error("Could not read cassandra schema string: " + e.getMessage());
            return null;
        }
    }

    /**
     * Gets the table data. If not available, fill it for the first time.
     * @param collectionName
     * @return
     */
    private TableData getTableData(String collectionName)
    {
        TableData tableData = tablesData.get(collectionName);

        if (tableData != null)
        {
            return tableData;
        }

        // This table is not yet known, grab its schema from Cassandra if it exists

        Metadata metadata = session.getMetadata();

        LinkedHashMap<String, CassandraType> tableSchema = parseSchemaForCollection(metadata, collectionName);

        if (tableSchema == null)
        {
            logger.error("Could not find schema for table " + collectionName + " in cassandra");
            return null;
        }

        BoundStatement boundStatement = prepareStatement(collectionName, tableSchema);
        tableData = new TableData(boundStatement, tableSchema);

        synchronized(tablesData) {
            tablesData.put(collectionName, tableData);
        }

        return tableData;
    }

    void stop()
    {
        stop = true;
    }

    /**
     * All what's needed to index a record
     */
    private static class DataToInsert
    {
        String collectionName; // Table name
        BoundStatement boundStatement; // Prepared statement
        List<Object> values; // Values to put in the preparded statement
    }

    @Override
    public void run() {

        List<DataToInsert> batchValues = new ArrayList<DataToInsert>();

        while (!stop) {

            try {
                // Process record if one
                try {
                    RecordToIndex recordToIndex = records.poll(flushInterval, TimeUnit.MILLISECONDS);
                    if (recordToIndex != null) {
                        String collectionName = recordToIndex.getCollectionName();
                        Record record = recordToIndex.getRecord();
                        TableData tableData = getTableData(collectionName);
                        if (tableData == null)
                        {
                            if (!collectionName.equals(END_OF_TEST)) {
                                record.addError(ProcessError.UNKNOWN_ERROR.toString(),
                                        "Cannot find cassandra collection definition for collection " + collectionName);
                                logger.error("\"Cannot find cassandra collection definition for collection " + collectionName +
                                        " for this record: " + record);
                            } else
                            {
                                // Collection is END_OF_TEST
                                // Special code for unit test. Signal the end of the unit test stream
                                DataToInsert dataToInsert = new DataToInsert();
                                dataToInsert.collectionName = collectionName;
                                batchValues.add(dataToInsert);
                            }
                        } else
                        {
                            DataToInsert dataToInsert = new DataToInsert();
                            dataToInsert.collectionName = collectionName;
                            dataToInsert.boundStatement = tableData.getBoundStatement();
                            dataToInsert.values = RecordConverter.convertInsert(record, tableData.getOrderedFields());
                            batchValues.add(dataToInsert);
                            batchedUpdates++;
                        }
                    }
                } catch (InterruptedException e) {
                    // Here we should exit the loop
                    logger.warn("Interrupted while waiting", e);
                    break;
                }

                // If time to do so, insert records into Cassandra
                long currentTS = System.nanoTime();
                if (lastTS == 0L) // Insure we wait for the flush interval when first time entering the loop
                {
                    lastTS = currentTS;
                }
                if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                    logger.debug("committing {} records to Cassandra after {} ns", batchedUpdates, (currentTS - lastTS));
                    executeInserts(batchValues);
                    lastTS = currentTS;
                    batchedUpdates = 0;
                    batchValues.clear();
                }
            } catch (Throwable t) {
                logger.error("Error in cassandra updater: " + t.getMessage());
                throw new RuntimeException(t);
            }
        }
    }

    private void executeInserts(List<DataToInsert> batchValues)
    {
        for (DataToInsert dataToInsert : batchValues)
        {
            // Special tets for unit test
            if (dataToInsert.collectionName.equals(END_OF_TEST))
            {
                // Signal end of test stream
                service.stillSomeRecords = false; // We suppose the unit test code is mono-threaded and never writes more that a batch can handle
                continue;
            }

            List<Object> insertValues = dataToInsert.values;
            BoundStatement boundStatement = dataToInsert.boundStatement;
            logger.debug("Cassandra inserting values: " + insertValues + " for bound statement: " + boundStatement);
            Object[] values = new Object[insertValues.size()];
            values = insertValues.toArray(values);
            try {
                boundStatement = boundStatement.getPreparedStatement().bind(values);
                // Handle null values: unset them to avoid unwanted tombstones. See #450
                int i = 0;
                for (Object value : values)
                {
                    if (value == null)
                    {
                        boundStatement = boundStatement.unset(i);
                    }
                    i++;
                }
                ResultSet resultSet = session.execute(boundStatement);
                if (!resultSet.wasApplied()) {
                    logger.error("Error inserting " + insertValues);
                }
            } catch(DriverException e) {
                logger.error("Error inserting " + insertValues + ": " + e.getMessage());
            }
        }
    }
}