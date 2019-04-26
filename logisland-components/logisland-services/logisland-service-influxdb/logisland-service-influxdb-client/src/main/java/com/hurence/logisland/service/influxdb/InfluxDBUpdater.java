/**
 * Copyright (C) 2019 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.influxdb;

import com.hurence.logisland.record.Record;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import com.hurence.logisland.service.influxdb.InfluxDBControllerService.RecordToIndex;

import static com.hurence.logisland.service.influxdb.InfluxDBControllerService.END_OF_TEST;

/**
 * This is a Runnable class used to buffer record to bulk put into InfluxDB
 */
public class InfluxDBUpdater implements Runnable {

    private InfluxDBControllerService service;
    private final BlockingQueue<RecordToIndex> records;
    private final int batchSize;
    private final long flushInterval;
    private volatile int batchedUpdates = 0;
    private volatile long lastTS = 0L;
    volatile boolean stop = false;

    private InfluxDB influxDB;

    private static volatile int threadCount = 0;

    private Logger logger = LoggerFactory.getLogger(InfluxDBUpdater.class.getName() + threadCount);

    public InfluxDBUpdater(InfluxDB influxDB, BlockingQueue<RecordToIndex> records, int batchSize,
                            InfluxDBControllerService service, long flushInterval) {
        this.influxDB = influxDB;
        this.records = records;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.service = service;
        threadCount++;
    }

    void stop()
    {
        stop = true;
    }

    enum InfluxDBType
    {
        INTEGER, FLOAT, STRING, BOOLEAN
    }

    static class InfluxDBField
    {
        private InfluxDBType type;
        private Object value;

        public InfluxDBField(InfluxDBType type, Object value)
        {
            this.type = type;
            this.value = value;
        }

        public InfluxDBType getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }
    }

    /**
     * All what's needed to index a record
     */
    static class DataToInsert
    {
        String measurement;
        Map<String, InfluxDBField> fields = new HashMap<String, InfluxDBField>();
        Map<String, String> tags = new HashMap<String, String>();
        long time;
        TimeUnit format;

        public String toString()
        {
            return "[measurement: " + measurement +
                    ", fields: " + fields +
                    ", tags: " + tags +
                    ", time: " + time +
                    ", format: " + format + "]";
        }
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
                        String measurement = recordToIndex.getMeasurement();
                        Record record = recordToIndex.getRecord();
                        DataToInsert dataToInsert = RecordConverter.convertToInfluxDB(
                                record,
                                measurement,
                                service.getMode(),
                                service.getTags().get(measurement),
                                service.getFields().get(measurement),
                                service.getTimeFields().get(measurement));
                        if (dataToInsert.fields.isEmpty() && !dataToInsert.measurement.equals(END_OF_TEST)) {
                            logger.warn("Skipping this record as it has no InfluxDB fields: " + record);
                        } else {
                            batchValues.add(dataToInsert);
                            batchedUpdates++;
                        }
                    }
                } catch (InterruptedException e) {
                    // Here we should exit the loop
                    logger.warn("Interrupted while waiting", e);
                    break;
                }

                // If time to do so, insert records into InfluxDB
                long currentTS = System.nanoTime();
                if (lastTS == 0L) // Insure we wait for the flush interval when first time entering the loop
                {
                    lastTS = currentTS;
                }
                if ((currentTS - lastTS) >= flushInterval * 1000000 || batchedUpdates >= batchSize) {
                    logger.debug("committing {} records to influxdb after {} ns", batchedUpdates, (currentTS - lastTS));
                    writeToInfluxDB(batchValues);
                    lastTS = currentTS;
                    batchedUpdates = 0;
                    batchValues.clear();
                }
            } catch (Throwable t) {
                logger.error("Error in influxdb updater: " + t.getMessage());
            }
        }
    }

    /**
     * Writes the provided bunch of data into InfluxDB
     * @param batchValues
     */
    private void writeToInfluxDB(List<DataToInsert> batchValues)
    {
        BatchPoints pointsBatch = BatchPoints
                .database(service.getDatabase())
                .build();

        boolean endOfTestFound = false;
        for (DataToInsert dataToInsert : batchValues)
        {
            // Special test for unit test
            if (dataToInsert.measurement.equals(END_OF_TEST))
            {
                endOfTestFound = true;
                // Must signal end of test stream after having written the batch to influxDB
                continue;
            }

            Point.Builder pointBuilder = Point.measurement(dataToInsert.measurement);

            // Add fields
            for (Map.Entry<String, InfluxDBField> entry : dataToInsert.fields.entrySet())
            {
                String fieldName = entry.getKey();
                InfluxDBField influxDBField = entry.getValue();
                InfluxDBType type = influxDBField.getType();
                Object value = influxDBField.getValue();
                switch (type)
                {
                    case INTEGER:
                        pointBuilder.addField(fieldName, (long)value);
                        break;
                    case FLOAT:
                        pointBuilder.addField(fieldName, (double)value);
                        break;
                    case STRING:
                        pointBuilder.addField(fieldName, (String)value);
                        break;
                    case BOOLEAN:
                        pointBuilder.addField(fieldName, (boolean)value);
                        break;
                    default:
                        throw new RuntimeException("Unsupported InfluxDBType: " + type);
                }
            }

            // Add tags
            pointBuilder.tag(dataToInsert.tags);

            // Add time
            /**
             * WARNING: if using record_time or another configured time field, : each record_time value should be
             * different in the batch or only one point is kept among points sharing the same time in the batch.
             */
            pointBuilder.time(dataToInsert.time, dataToInsert.format);

            pointsBatch.point(pointBuilder.build());
        }
        try {
            influxDB.write(pointsBatch);
        } catch(Throwable t)
        {
            logger.error("Error inserting " + batchValues + ": " + t.getMessage());
        }

        // Special test for unit test
        if (endOfTestFound) {
            // Signal end of test stream
            service.stillSomeRecords = false; // We suppose the unit test code is mono-threaded and never writes more than a batch can handle
        }
    }
}
