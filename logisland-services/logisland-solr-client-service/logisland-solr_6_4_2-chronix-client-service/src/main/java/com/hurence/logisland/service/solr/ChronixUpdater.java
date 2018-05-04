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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ChronixUpdater implements Runnable {

    private final SolrClient solr;
    private final BlockingQueue<Record> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0;
    private final Map<String, String> fieldToMetricTypeMapping = new HashMap<>();

    private static volatile int threadCount = 0;
    protected static Function<MetricTimeSeries, String> groupBy = MetricTimeSeries::getName;
    protected static BinaryOperator<MetricTimeSeries> reduce = (binaryTimeSeries, binaryTimeSeries2) -> binaryTimeSeries;

    private Logger logger = LoggerFactory.getLogger(ChronixUpdater.class.getName() + threadCount);

    private MetricTimeSeriesConverter converter = null;
    private ChronixSolrStorage<MetricTimeSeries> storage = null;

    public ChronixUpdater(SolrClient solr, BlockingQueue<Record> records, Map<String, String> fieldToMetricTypeMapping,
                          int batchSize, long flushInterval) {
        this.solr = solr;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.lastTS = System.nanoTime(); // far in the future ...
        converter = new MetricTimeSeriesConverter();
        storage = new ChronixSolrStorage<>(batchSize, groupBy, reduce);
        if (fieldToMetricTypeMapping != null) {
            this.fieldToMetricTypeMapping.putAll(fieldToMetricTypeMapping);
        }
        //add the defaults
        this.fieldToMetricTypeMapping.put(FieldDictionary.RECORD_VALUE, RecordDictionary.METRIC);
        threadCount++;
    }

    @Override
    public void run() {
        List<Record> batchBuffer = new ArrayList<>();

        while (true) {

            // process record if one
            try {
                Record record = records.take();
                if (record != null) {
                    batchBuffer.add(record);
                    batchedUpdates++;
                }
            } catch (InterruptedException e) {
                //here we should exit the loop
                logger.warn("Interrupted while waiting", e);
                break;
            }

            //
            try {
                long currentTS = System.nanoTime();
                if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                    //use moustache operator to avoid composing strings when not needed
                    logger.debug("committing {} records to Chronix after {} ns", batchedUpdates, (currentTS - lastTS));
                    storage.add(converter, convertToMetric(batchBuffer), solr);
                    solr.commit();
                    lastTS = currentTS;
                    batchBuffer = new ArrayList<>();
                    batchedUpdates = 0;
                }


                // Thread.sleep(10);
            } catch (IOException | SolrServerException e) {
                logger.error("Unexpected I/O exception", e);
            }
        }
    }

    List<MetricTimeSeries> convertToMetric(List<Record> records) throws DatastoreClientServiceException {


        Record first = records.get(0);
        String batchUID = UUID.randomUUID().toString();
        final long firstTS = records.get(0).getTime().getTime();
        long tmp = records.get(records.size() - 1).getTime().getTime();
        final long lastTS = tmp == firstTS ? firstTS + 1 : firstTS;


        //extract meta
        String metricName = first.getField(FieldDictionary.RECORD_NAME).asString();
        Map<String, Object> attributes = first.getAllFieldsSorted().stream()
                .filter(field -> !fieldToMetricTypeMapping.containsKey(field.getName()))
                .filter(field -> !field.getName().equals(FieldDictionary.RECORD_TIME) &&
                        !field.getName().equals(FieldDictionary.RECORD_NAME) &&
                        !field.getName().equals(FieldDictionary.RECORD_VALUE) &&
                        !field.getName().equals(FieldDictionary.RECORD_ID) &&
                        !field.getName().equals(FieldDictionary.RECORD_TYPE)
                )
                .collect(Collectors.toMap(field -> field.getName().replaceAll("\\.", "_"),
                        field -> {
                            try {
                                switch (field.getType()) {
                                    case STRING:
                                        return field.asString();
                                    case INT:
                                        return field.asInteger();
                                    case LONG:
                                        return field.asLong();
                                    case FLOAT:
                                        return field.asFloat();
                                    case DOUBLE:
                                        return field.asDouble();
                                    case BOOLEAN:
                                        return field.asBoolean();
                                    default:
                                        return field.getRawValue();
                                }
                            } catch (Exception e) {
                                logger.error("Unable to process field " + field, e);
                                return null;
                            }
                        }
                ));

        return fieldToMetricTypeMapping.entrySet().stream()
                .map(entry -> {
                            MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(metricName, entry.getValue());
                            List<Map.Entry<Long, Double>> points = records.stream()
                                    .filter(record -> record.hasField(entry.getKey()) && record.getField(entry.getKey()).isSet())
                                    .map(record ->
                                            new AbstractMap.SimpleEntry<>(record.getTime().getTime(),
                                                    record.getField(entry.getKey()).asDouble())
                                    ).collect(Collectors.toList());
                            if (points.isEmpty()) {
                                return null;
                            }
                            points.stream().forEach(kv -> builder.point(kv.getKey(), kv.getValue()));

                            return builder
                                    .attribute("id", batchUID.toString())
                                    .start(firstTS)
                                    .end(lastTS)
                                    .attributes(attributes)
                                    .build();
                        }
                ).filter(a -> a != null)
                .collect(Collectors.toList());
    }

}
