/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import de.qaware.chronix.timeseries.dts.Pair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
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
    private final Collection<String> groupByFields = new LinkedHashSet<>();
    private final List<Record> batchBuffer = new ArrayList<>();


    private static volatile int threadCount = 0;
    protected static Function<MetricTimeSeries, String> storageGroupBy = MetricTimeSeries::getName;
    protected static BinaryOperator<MetricTimeSeries> reduce = (binaryTimeSeries, binaryTimeSeries2) -> binaryTimeSeries;

    private Logger logger = LoggerFactory.getLogger(ChronixUpdater.class.getName() + threadCount);

    private MetricTimeSeriesConverter converter = null;
    private ChronixSolrStorage<MetricTimeSeries> storage = null;

    public ChronixUpdater(SolrClient solr, BlockingQueue<Record> records, List<String> groupByFields,
                          int batchSize, long flushInterval) {
        this.solr = solr;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.lastTS = System.nanoTime(); // far in the future ...
        converter = new MetricTimeSeriesConverter();
        storage = new ChronixSolrStorage<>(batchSize, storageGroupBy, reduce);
        if (groupByFields != null) {
            this.groupByFields.addAll(groupByFields);
        }
        threadCount++;
    }

    @Override
    public void run() {

        while (true) {

            // process record if one
            try {
                Record record = records.poll(flushInterval, TimeUnit.MILLISECONDS);
                if (record != null) {
                    batchBuffer.add(record);
                    batchedUpdates++;
                }
            } catch (InterruptedException e) {
                //here we should exit the loop
                logger.warn("Interrupted while waiting", e);
                try {
                    sendData(System.nanoTime());
                } catch (Exception e1) {
                    logger.warn("Unable to flush data before quitting.", e1);
                }
                break;
            }

            //
            try {
                long currentTS = System.nanoTime();
                if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                    sendData(currentTS);
                }


                // Thread.sleep(10);
            } catch (IOException | SolrServerException e) {
                logger.error("Unexpected I/O exception", e);
            }
        }
    }

    private synchronized void sendData(long currentTS) throws SolrServerException, IOException {
        logger.debug("committing {} records to Chronix after {} ns", batchedUpdates, (currentTS - lastTS));
        Map<String, List<Record>> groups = batchBuffer.stream().collect(Collectors.groupingBy(r ->
                groupByFields.stream().map(f -> r.hasField(f) ? r.getField(f).asString() : null).collect(Collectors.joining("|"))));
        if (!groups.isEmpty()) {
            storage.add(converter,
                    groups.values().stream().filter(l -> !l.isEmpty()).map(recs -> {
                        Collections.sort(recs, Comparator.comparing(Record::getTime));
                        return recs;
                    }).map(this::convertToMetric).collect(Collectors.toList()), solr);
            solr.commit();
        }
        lastTS = currentTS;
        batchBuffer.clear();
        batchedUpdates = 0;
    }

    MetricTimeSeries convertToMetric(List<Record> records) throws DatastoreClientServiceException {


        Record first = records.get(0);
        String batchUID = UUID.randomUUID().toString();
        final long firstTS = records.get(0).getTime().getTime();
        long tmp = records.get(records.size() - 1).getTime().getTime();
        final long lastTS = tmp == firstTS ? firstTS + 1 : tmp;


        //extract meta
        String metricType = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_TYPE) &&
                record.getField(FieldDictionary.RECORD_TYPE).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_TYPE).asString())
                .findFirst().orElse(RecordDictionary.METRIC);

        String metricName = records.stream().filter(record -> record.hasField(FieldDictionary.RECORD_NAME) &&
                record.getField(FieldDictionary.RECORD_NAME).getRawValue() != null)
                .map(record -> record.getField(FieldDictionary.RECORD_NAME).asString())
                .findFirst().orElse("unknown");

        Map<String, Object> attributes = first.getAllFieldsSorted().stream()
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

        MetricTimeSeries.Builder ret = new MetricTimeSeries.Builder(metricName, metricType)
                .attributes(attributes)
                .attribute("id", batchUID)
                .start(firstTS)
                .end(lastTS);

        records.stream()
                .filter(record -> record.getField(FieldDictionary.RECORD_VALUE) != null && record.getField(FieldDictionary.RECORD_VALUE).getRawValue() != null)
                .map(record -> new Pair<>(record.getTime().getTime(), record.getField(FieldDictionary.RECORD_VALUE).asDouble()))
                .filter(longDoublePair -> longDoublePair.getSecond() != null && Double.isFinite(longDoublePair.getSecond()))
                .forEach(pair -> ret.point(pair.getFirst(), pair.getSecond()));


        return ret.build();

    }

}
