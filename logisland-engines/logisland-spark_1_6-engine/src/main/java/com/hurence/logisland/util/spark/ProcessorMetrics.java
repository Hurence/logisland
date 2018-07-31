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
package com.hurence.logisland.util.spark;


import com.hurence.logisland.metrics.Names;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Created by tom on 09/09/16.
 */
public class ProcessorMetrics {
    private static Logger logger = LoggerFactory.getLogger(ProcessorMetrics.class.getName());

    public synchronized static void resetMetrics(final String metricPrefix) {

        logger.info("reseting metrics " + metricPrefix);
        UserMetricsSystem.gauge(metricPrefix + Names.INCOMING_MESSAGES).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.INCOMING_RECORDS).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.OUTGOING_RECORDS).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.ERRORS).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_FIELD_AVERAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_RECORD_AVERAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.RECORDS_PER_SECOND_AVERAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.PROCESSED_BYTES).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.PROCESSED_FIELDS).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.ERROR_PERCENTAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.FIELDS_PER_RECORD_AVERAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_SECOND_AVERAGE).set(0);
        UserMetricsSystem.gauge(metricPrefix + Names.PROCESSING_TIME_MS).set(0);
    }


    /**
     * publish
     *
     * @param metricPrefix
     * @param incomingEvents
     * @param outgoingEvents
     * @param fromOffset
     * @param untilOffset
     * @param processingDurationInMillis
     */
    public synchronized static void computeMetrics(
            final String metricPrefix,
            final Collection<Record> incomingEvents,
            final Collection<Record> outgoingEvents,
            final long fromOffset,
            final long untilOffset,
            final long processingDurationInMillis) {


        if ((outgoingEvents != null) && (outgoingEvents.size() != 0)) {

            UserMetricsSystem.gauge(metricPrefix + Names.INCOMING_MESSAGES).set(untilOffset - fromOffset);
            UserMetricsSystem.gauge(metricPrefix + Names.INCOMING_RECORDS).set(incomingEvents.size());
            UserMetricsSystem.gauge(metricPrefix + Names.OUTGOING_RECORDS).set(outgoingEvents.size());

            long errorCount = outgoingEvents.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
            UserMetricsSystem.gauge(metricPrefix + Names.ERRORS).set(errorCount);
            if (outgoingEvents.size() != 0) {
                final List<Integer> recordSizesInBytes = new ArrayList<>();
                final List<Integer> recordNumberOfFields = new ArrayList<>();

                outgoingEvents.forEach(record -> {
                    recordSizesInBytes.add(record.sizeInBytes());
                    recordNumberOfFields.add(record.size());
                });

                final int numberOfProcessedBytes = recordSizesInBytes.stream().mapToInt(Integer::intValue).sum();
                final int numberOfProcessedFields = recordNumberOfFields.stream().mapToInt(Integer::intValue).sum();

                if (numberOfProcessedFields != 0) {
                    UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_FIELD_AVERAGE).set(numberOfProcessedBytes / numberOfProcessedFields);
                } else {
                    UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_FIELD_AVERAGE).set(0);
                }
                if (processingDurationInMillis != 0) {
                    UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_SECOND_AVERAGE).set(numberOfProcessedBytes * 1000 / processingDurationInMillis);
                    UserMetricsSystem.gauge(metricPrefix + Names.RECORDS_PER_SECOND_AVERAGE).set(outgoingEvents.size() * 1000 / processingDurationInMillis);
                } else {
                    UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_SECOND_AVERAGE).set(0);
                    UserMetricsSystem.gauge(metricPrefix + Names.RECORDS_PER_SECOND_AVERAGE).set(0);
                }


                UserMetricsSystem.gauge(metricPrefix + Names.PROCESSED_BYTES).set(numberOfProcessedBytes);
                UserMetricsSystem.gauge(metricPrefix + Names.PROCESSED_FIELDS).set(numberOfProcessedFields);

                UserMetricsSystem.gauge(metricPrefix + Names.ERROR_PERCENTAGE).set((long) (100.0f * errorCount / outgoingEvents.size()));
                UserMetricsSystem.gauge(metricPrefix + Names.FIELDS_PER_RECORD_AVERAGE).set(numberOfProcessedFields / outgoingEvents.size());
                UserMetricsSystem.gauge(metricPrefix + Names.BYTES_PER_RECORD_AVERAGE).set(numberOfProcessedBytes / outgoingEvents.size());
            } else if (errorCount > 0)
                UserMetricsSystem.gauge(metricPrefix + Names.ERROR_PERCENTAGE).set(100L);
            else
                UserMetricsSystem.gauge(metricPrefix + Names.ERROR_PERCENTAGE).set(0L);


            UserMetricsSystem.gauge(metricPrefix + Names.PROCESSING_TIME_MS).set(processingDurationInMillis);

        }
    }
}
