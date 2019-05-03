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

import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.Tuple;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * this is a Runnable class used to buffer record to bulk put into Mongo
 */
public class MongoDBUpdater implements Runnable {

    private static volatile int threadCount = 0;
    private final BlockingQueue<Tuple<Record, Bson>> records;
    private final int batchSize;
    private final long flushInterval;
    private long lastTS = 0;
    private final String bulkMode;
    private final MongoDatabase db;
    private final MongoCollection<Document> col;


    private Logger logger = LoggerFactory.getLogger(MongoDBUpdater.class.getName() + threadCount);


    public MongoDBUpdater(MongoDatabase db,
                          MongoCollection<Document> col,
                          BlockingQueue<Tuple<Record, Bson>> records,
                          int batchSize,
                          long flushInterval,
                          String bulkMode) {
        this.db = db;
        this.col = col;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.bulkMode = bulkMode;
        this.lastTS = System.nanoTime(); // far in the future ...
        threadCount++;
    }

    @Override
    public void run() {
        List<Tuple<Document, Bson>> batchBuffer = new ArrayList<>();

        while (true) {
            try {
                Tuple<Record, Bson> record = records.poll(flushInterval, TimeUnit.MILLISECONDS);
                if (record != null) {
                    batchBuffer.add(new Tuple<>(RecordConverter.convert(record.getKey()), record.getValue()));
                }
                long currentTS = System.nanoTime();
                if (batchBuffer.size() > 0 &&
                        ((currentTS - lastTS) >= flushInterval * 1000000 || batchBuffer.size() >= batchSize)) {
                    //use moustache operator to avoid composing strings when not needed
                    logger.debug("committing {} records to Mongo after {} ns", batchBuffer.size(), (currentTS - lastTS));

                    if (MongoDBControllerService.BULK_MODE_UPSERT.getValue().equals(bulkMode)) {
                        ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                        //split batches by 500 document each max
                        for (int i = 0; i < batchBuffer.size(); i+=500) {
                            try {
                                col.bulkWrite(batchBuffer.stream().skip(i).limit(500)
                                        .map(document -> new ReplaceOneModel<>(
                                                document.getValue(),
                                                document.getKey(),
                                                replaceOptions)).collect(Collectors.toList()));
                            } catch (MongoBulkWriteException bwe) {
                                bwe.getWriteErrors().forEach(error -> {
                                    if (error.getCode() != 11000) {
                                        logger.warn("MongoDB updater got error: {}", error);
                                    }
                                });
                            }
                        }
                    } else {
                        col.insertMany(batchBuffer.stream().map(Tuple::getKey).collect(Collectors.toList()));
                    }
                    lastTS = currentTS;
                    batchBuffer = new ArrayList<>();
                }

            } catch (InterruptedException e) {
                //here we should exit the loop
                logger.info("Interrupted while waiting: {}", e.getMessage());
                break;
            } catch (Exception e) {
                logger.error("Unrecoverable error from MongoDB updater. Loosing data!", e);
                batchBuffer.clear();
                lastTS = System.nanoTime();
            }
        }
    }

}



