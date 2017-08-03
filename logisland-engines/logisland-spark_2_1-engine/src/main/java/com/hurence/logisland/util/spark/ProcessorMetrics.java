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
package com.hurence.logisland.util.spark;


import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.apache.spark.groupon.metrics.UserMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;


/**
 * Created by tom on 09/09/16.
 */
public class ProcessorMetrics {
    private static Logger logger = LoggerFactory.getLogger(ProcessorMetrics.class.getName());

    public synchronized static void resetMetrics(final String metricPrefix) {

        logger.info("reseting metrics " +metricPrefix );
        UserMetricsSystem.meter(metricPrefix + "numIncomingMessages").mark(0);
        UserMetricsSystem.meter(metricPrefix + "numIncomingRecords").mark(0);
        UserMetricsSystem.meter(metricPrefix + "numOutgoingMessages").mark(0);
        UserMetricsSystem.meter(metricPrefix + "numErrorRecords").mark(0);
        UserMetricsSystem.meter(metricPrefix + "avgBytesPerField").mark(0);
        UserMetricsSystem.meter(metricPrefix + "avgBytesPerSecond").mark(0);
        UserMetricsSystem.meter(metricPrefix + "avgRecordsPerSecond").mark(0);
        UserMetricsSystem.meter(metricPrefix + "numProcessedBytes").mark(0);
        UserMetricsSystem.meter(metricPrefix + "numProcessedFields").mark(0);
        UserMetricsSystem.meter(metricPrefix + "percentErrors").mark(0);
        UserMetricsSystem.meter(metricPrefix + "avgFieldsPerRecord").mark(0);
        UserMetricsSystem.meter(metricPrefix + "avgBytesPerRecord").mark(0);
        UserMetricsSystem.meter(metricPrefix + "processingTimeMs").mark(0);
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

            UserMetricsSystem.meter(metricPrefix + "numIncomingMessages").mark(untilOffset - fromOffset);
            UserMetricsSystem.meter(metricPrefix + "numIncomingRecords").mark(incomingEvents.size());
            UserMetricsSystem.meter(metricPrefix + "numOutgoingMessages").mark(outgoingEvents.size());

            long errorCount = outgoingEvents.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
            UserMetricsSystem.meter(metricPrefix + "numErrorRecords").mark(errorCount);
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
                    UserMetricsSystem.meter(metricPrefix + "avgBytesPerField").mark(numberOfProcessedBytes / numberOfProcessedFields);
                } else {
                    UserMetricsSystem.meter(metricPrefix + "avgBytesPerField").mark(0);
                }
                if (processingDurationInMillis != 0) {
                    UserMetricsSystem.meter(metricPrefix + "avgBytesPerSecond").mark(numberOfProcessedBytes * 1000 / processingDurationInMillis);
                    UserMetricsSystem.meter(metricPrefix + "avgRecordsPerSecond").mark(outgoingEvents.size() * 1000 / processingDurationInMillis);
                } else {
                    UserMetricsSystem.meter(metricPrefix + "avgBytesPerSecond").mark(0);
                    UserMetricsSystem.meter(metricPrefix + "avgRecordsPerSecond").mark(0);
                }


                UserMetricsSystem.meter(metricPrefix + "numProcessedBytes").mark(numberOfProcessedBytes);
                UserMetricsSystem.meter(metricPrefix + "numProcessedFields").mark(numberOfProcessedFields);

                UserMetricsSystem.meter(metricPrefix + "percentErrors").mark((long) (100.0f * errorCount / outgoingEvents.size()));
                UserMetricsSystem.meter(metricPrefix + "avgFieldsPerRecord").mark(numberOfProcessedFields / outgoingEvents.size());
                UserMetricsSystem.meter(metricPrefix + "avgBytesPerRecord").mark(numberOfProcessedBytes / outgoingEvents.size());
            } else if (errorCount > 0)
                UserMetricsSystem.meter(metricPrefix + "percentErrors").mark(100L);
            else
                UserMetricsSystem.meter(metricPrefix + "percentErrors").mark(0L);


            UserMetricsSystem.meter(metricPrefix + "processingTimeMs").mark(processingDurationInMillis);

        }
    }
}
