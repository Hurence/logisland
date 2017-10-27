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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Created by tom on 09/09/16.
 */
public class ProcessorMetrics {
    private static Logger logger = LoggerFactory.getLogger(ProcessorMetrics.class.getName());

    public synchronized static void resetMetrics(final String metricPrefix) {
        UserMetricsSystem.gauge(metricPrefix + "incoming_messages").set(0);
        UserMetricsSystem.gauge(metricPrefix + "incoming_records").set(0);
        UserMetricsSystem.gauge(metricPrefix + "outgoing_records").set(0);
        UserMetricsSystem.gauge(metricPrefix + "errors").set(0);
        UserMetricsSystem.gauge(metricPrefix + "bytes_per_field_average").set(0);
        UserMetricsSystem.gauge(metricPrefix + "bytes_per_record_average").set(0);
        UserMetricsSystem.gauge(metricPrefix + "records_per_second_average").set(0);
        UserMetricsSystem.gauge(metricPrefix + "processed_bytes").set(0);
        UserMetricsSystem.gauge(metricPrefix + "processed_fields").set(0);
        UserMetricsSystem.gauge(metricPrefix + "error_percentage").set(0);
        UserMetricsSystem.gauge(metricPrefix + "fields_per_record_average").set(0);
        UserMetricsSystem.gauge(metricPrefix + "bytes_per_second_average").set(0);
        //UserMetricsSystem.gauge(metricPrefix + "processing_time_ms").set(0);
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

            UserMetricsSystem.gauge(metricPrefix + "incoming_messages").set(untilOffset - fromOffset);
            UserMetricsSystem.gauge(metricPrefix + "incoming_records").set(incomingEvents.size());
            UserMetricsSystem.gauge(metricPrefix + "outgoing_records").set(outgoingEvents.size());

            long errorCount = outgoingEvents.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
            UserMetricsSystem.gauge(metricPrefix + "errors").set(errorCount);
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
                    UserMetricsSystem.gauge(metricPrefix + "bytes_per_field_average").set(numberOfProcessedBytes / numberOfProcessedFields);
                } else {
                    UserMetricsSystem.gauge(metricPrefix + "bytes_per_field_average").set(0);
                }
                if (processingDurationInMillis != 0) {
                    UserMetricsSystem.gauge(metricPrefix + "bytes_per_second_average").set(numberOfProcessedBytes * 1000 / processingDurationInMillis);
                    UserMetricsSystem.gauge(metricPrefix + "records_per_second_average").set(outgoingEvents.size() * 1000 / processingDurationInMillis);
                } else {
                    UserMetricsSystem.gauge(metricPrefix + "bytes_per_second_average").set(0);
                    UserMetricsSystem.gauge(metricPrefix + "records_per_second_average").set(0);
                }


                UserMetricsSystem.gauge(metricPrefix + "processed_bytes").set(numberOfProcessedBytes);
                UserMetricsSystem.gauge(metricPrefix + "processed_fields").set(numberOfProcessedFields);

                UserMetricsSystem.gauge(metricPrefix + "error_percentage").set((long) (100.0f * errorCount / outgoingEvents.size()));
                UserMetricsSystem.gauge(metricPrefix + "fields_per_record_average").set(numberOfProcessedFields / outgoingEvents.size());
                UserMetricsSystem.gauge(metricPrefix + "bytes_per_record_average").set(numberOfProcessedBytes / outgoingEvents.size());
            } else if (errorCount > 0)
                UserMetricsSystem.gauge(metricPrefix + "error_percentage").set(100L);
            else
                UserMetricsSystem.gauge(metricPrefix + "error_percentage").set(0L);


            //   UserMetricsSystem.gauge(metricPrefix + "processing_time_ms").set(processingDurationInMillis);

        }
    }
}
