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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;


/**
 * This is a Runnable class used to buffer record to bulk put into Cassandra
 */
public class CassandraUpdater implements Runnable {

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


    public CassandraUpdater(Cluster cluster, Session session, String keyspace, String table, BlockingQueue<Record> records, int batchSize,
                            long flushInterval) {
        this.cluster = cluster;
        this.session = session;
        this.keyspace = keyspace;
        this.table = table;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        threadCount++;
    }

    void stop()
    {
        stop = true;
    }

    @Override
    public void run() {
        List<String> batchInsertStatements = new ArrayList<String>();

        while (!stop) {

            try {

                // process record if one
                try {
                    Record record = records.take();
                    if (record != null) {
                        batchInsertStatements.add(RecordConverter.convertInsert(record, keyspace, table));
                        batchedUpdates++;
                    }
                } catch (InterruptedException e) {
                    //here we should exit the loop
                    logger.warn("Interrupted while waiting", e);
                    break;
                }

                // If time to do so, insert records into Cassandra
                long currentTS = System.nanoTime();
                if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                    logger.debug("committing {} records to Cassandra after {} ns", batchedUpdates, (currentTS - lastTS));
                    executeInserts(batchInsertStatements);
                    lastTS = currentTS;
                    batchedUpdates = 0;
                    batchInsertStatements.clear();
                }
            } catch(Throwable t)
            {
                logger.error("Error in cassandra updater: " + t.getMessage());
            }
        }
    }

    private void executeInserts(List<String> insertStatements)
    {
        StringBuffer sb = new StringBuffer();
        insertStatements.forEach(insertStatement -> {
            sb.append(insertStatement).append(";\n");
        });

        String cql = sb.toString();

        // TODO: use debug level
        logger.info("Cassandra updater executing statement: [" + cql + "]");
        System.out.println("Cassandra updater executing statement: [" + cql + "]");

        ResultSet resultSet = session.execute(cql);
        if (resultSet.wasApplied())
        {
            logger.error("Error executing statement: [" + cql + "]");
        }
    }
}
