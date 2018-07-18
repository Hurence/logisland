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
package com.hurence.logisland.service.mongodb;

import com.hurence.logisland.record.Record;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;


/**
 * this is a Runnable class used to buffer record to bulk put into Mongo
 */
public class MongoDBUpdater implements Runnable {


    private final BlockingQueue<Record> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0;

    private final MongoDatabase db;
    private final MongoCollection<Document> col;

    private static volatile int threadCount = 0;

    private Logger logger = LoggerFactory.getLogger(MongoDBUpdater.class.getName() + threadCount);


    public MongoDBUpdater(MongoDatabase db,
                          MongoCollection<Document> col,
                          BlockingQueue<Record> records,
                          int batchSize,
                          long flushInterval) {
        this.db = db;
        this.col = col;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.lastTS = System.nanoTime(); // far in the future ...
        threadCount++;
    }

    @Override
    public void run() {
        List<Document> batchBuffer = new ArrayList<>();

        while (true) {

            // process record if one
            try {
                Record record = records.take();
                if (record != null) {
                    batchBuffer.add(RecordConverter.convert(record));
                    batchedUpdates++;
                }
            } catch (InterruptedException e) {
                //here we should exit the loop
                logger.warn("Interrupted while waiting", e);
                break;
            }

            //
            long currentTS = System.nanoTime();
            if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                //use moustache operator to avoid composing strings when not needed
                logger.debug("committing {} records to Mongo after {} ns", batchedUpdates, (currentTS - lastTS));
                col.insertMany(batchBuffer);
                lastTS = currentTS;
                batchBuffer = new ArrayList<>();
                batchedUpdates = 0;
            }


            // Thread.sleep(10);
        }
    }


}
