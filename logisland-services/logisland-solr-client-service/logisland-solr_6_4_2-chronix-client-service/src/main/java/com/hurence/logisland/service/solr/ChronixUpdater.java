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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordDictionary;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import de.qaware.chronix.converter.MetricTimeSeriesConverter;
import de.qaware.chronix.solr.client.ChronixSolrStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class ChronixUpdater implements Runnable {

    private final SolrClient solr;
    private final BlockingQueue<Record> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0;

    private static volatile int threadCount = 0;
    protected static Function<MetricTimeSeries, String> groupBy = MetricTimeSeries::getName;
    protected static BinaryOperator<MetricTimeSeries> reduce = (binaryTimeSeries, binaryTimeSeries2) -> binaryTimeSeries;

    private Logger logger = LoggerFactory.getLogger(ChronixUpdater.class.getName() + threadCount);

    MetricTimeSeriesConverter converter = null;
    ChronixSolrStorage<MetricTimeSeries> storage = null;

    public ChronixUpdater(SolrClient solr, BlockingQueue<Record> records, int batchSize, long flushInterval) {
        this.solr = solr;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.lastTS = System.nanoTime(); // far in the future ...
        converter = new MetricTimeSeriesConverter();
        storage = new ChronixSolrStorage<>(batchSize, groupBy, reduce);
        threadCount++;
    }

    @Override
    public void run() {
        while (true) {

            // process record if one
            try {
                Record record = records.take();
                if (record != null) {

                    try {
                        MetricTimeSeries metric = convertToMetric(record);

                        List<MetricTimeSeries> timeSeries = new ArrayList<>();
                        timeSeries.add(metric);
                        storage.add(converter, timeSeries, solr);


                    } catch (DatastoreClientServiceException ex) {
                        logger.error(ex.toString() + " for record " + record.toString());
                    }

                    batchedUpdates++;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //
            try {
                long currentTS = System.nanoTime();
                if ((currentTS - lastTS)  >= flushInterval * 1000000  || batchedUpdates >= batchSize) {

                    logger.debug("commiting " + batchedUpdates + " records to Chronix after " + (currentTS - lastTS) + " ns");
                    solr.commit();
                    lastTS = currentTS;
                    batchedUpdates = 0;
                }


                // Thread.sleep(10);
            } catch (IOException | SolrServerException e) {
                e.printStackTrace();
            }
        }
    }

    MetricTimeSeries convertToMetric(Record record) throws DatastoreClientServiceException {

        try {
            long recordTS = record.getTime().getTime();

            MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(
                    record.getField(FieldDictionary.RECORD_NAME).asString(), RecordDictionary.METRIC)
                    .start(recordTS)
                    .end(recordTS + 10)
                    .attribute("id", record.getId())
                    .point(recordTS, record.getField(FieldDictionary.RECORD_VALUE).asDouble());


            // add all other records
            record.getAllFieldsSorted().forEach(field -> {
                try {
                    // cleanup invalid es fields characters like '.'
                    String fieldName = field.getName()
                            .replaceAll("\\.", "_");

                    if (!fieldName.equals(FieldDictionary.RECORD_TIME) &&
                            !fieldName.equals(FieldDictionary.RECORD_NAME) &&
                            !fieldName.equals(FieldDictionary.RECORD_VALUE) &&
                            !fieldName.equals(FieldDictionary.RECORD_ID) &&
                            !fieldName.equals(FieldDictionary.RECORD_TYPE))


                        switch (field.getType()) {

                            case STRING:
                                builder.attribute(fieldName, field.asString());
                                break;
                            case INT:
                                builder.attribute(fieldName, field.asInteger());
                                break;
                            case LONG:
                                builder.attribute(fieldName, field.asLong());
                                break;
                            case FLOAT:
                                builder.attribute(fieldName, field.asFloat());
                                break;
                            case DOUBLE:
                                builder.attribute(fieldName, field.asDouble());
                                break;
                            case BOOLEAN:
                                builder.attribute(fieldName, field.asBoolean());
                                break;
                            default:
                                builder.attribute(fieldName, field.getRawValue());
                                break;
                        }

                } catch (Throwable ex) {
                    logger.error("unable to process a field in record : {}, {}", record, ex.toString());
                }


            });

            return builder.build();
        } catch (Exception ex) {
            throw new DatastoreClientServiceException("bad record : " + ex.toString());
        }
    }

}
