/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * This is a Runnable class used to buffer record to bulk put into Cassandra
 */
public class CassandraUpdater implements Runnable {

    private CassandraControllerService service;
    private final BlockingQueue<Record> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0L;
    volatile boolean stop = false;

    private Cluster cluster;
    private Session session;
    private String keyspace;
    private String table;

    private static volatile int threadCount = 0;

    private Logger logger = LoggerFactory.getLogger(CassandraUpdater.class.getName() + threadCount);

    private PreparedStatement statement;
    private BoundStatement boundStatement;

    public CassandraUpdater(Cluster cluster, Session session, String keyspace, String table, BlockingQueue<Record> records, int batchSize,
                            CassandraControllerService service, long flushInterval) {
        this.cluster = cluster;
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.service = service;
        threadCount++;

        prepareStatement();
    }

    /**
     * Prepare once for all a bound statement that will be always used: this is faster than constructing and using a
     * specific string each time.
     */
    private void prepareStatement() {

        /**
         * INSERT INTO keyspace.table (field1, field2) VALUES (?, ?)
         */
        StringBuffer sb = new StringBuffer("INSERT INTO " + keyspace + "." + table + " (");
        StringBuffer questionMarksSb = new StringBuffer();
        boolean first = true;
        for (String field : service.fieldsToType.keySet()) {
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
        statement = session.prepare(statementString);
        boundStatement = new BoundStatement(statement);
    }

    void stop()
    {
        stop = true;
    }

    @Override
    public void run() {
        List<List<Object>> batchInsertValues = new ArrayList<List<Object>>();

        while (!stop) {
            try {

                // process record if one
                try {
                    Record record = records.poll(100, TimeUnit.MILLISECONDS);
                    if (record != null) {
                        batchInsertValues.add(RecordConverter.convertInsert(record, service.fieldsToType));
                        batchedUpdates++;
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
                    executeInserts(batchInsertValues);
                    lastTS = currentTS;
                    batchedUpdates = 0;
                    batchInsertValues.clear();
                    service.stillSomeRecords = false; // We suppose the unit test code is mono-threaded and never writes more that a batch can handle
                }
            } catch (Throwable t) {
                logger.error("Error in cassandra updater: " + t.getMessage());
            }
        }
    }

    private void executeInserts(List<List<Object>> insertValuesList)
    {
        insertValuesList.forEach(insertValues -> {
            //logger.debug("Cassandra inserting values: " + insertValues);
            Object[] values = new Object[service.fieldsToType.size()];
            values = insertValues.toArray(values);
            try {
                ResultSet resultSet = session.execute(boundStatement.bind(values));
                if (!resultSet.wasApplied()) {
                    logger.error("Error inserting " + insertValues);
                }
            } catch(DriverException e) {
                logger.error("Error inserting " + insertValues + ": " + e.getMessage());
            }
        });
    }
}