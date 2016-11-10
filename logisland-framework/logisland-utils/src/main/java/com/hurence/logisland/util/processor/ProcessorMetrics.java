/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.util.processor;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hurence.logisland.record.FieldType.INT;
import static com.hurence.logisland.record.FieldType.LONG;

/**
 * Created by tom on 09/09/16.
 */
public class ProcessorMetrics {

    public static String METRICS_EVENT_TYPE = "logisland_metrics";

    private static Logger logger = LoggerFactory.getLogger(ProcessorMetrics.class);


    public synchronized static Collection<Record> computeMetrics(
            final String appName,
            final String componentName,
            final String inputTopics,
            final String outputTopics,
            final int partitionId,
            final Collection<Record> incomingEvents,
            final Collection<Record> outgoingEvents,
            final long fromOffset,
            final long untilOffset,
            final long processingDurationInMillis) {


        if (outgoingEvents.size() != 0) {
            Record metrics = new StandardRecord(METRICS_EVENT_TYPE);

            metrics.setField("spark_app_name", FieldType.STRING, appName);
            metrics.setField("spark_partition_id", FieldType.INT, partitionId);
            metrics.setField("component_name", FieldType.STRING, componentName);
            metrics.setField("input_topics", FieldType.STRING, inputTopics);
            metrics.setField("output_topics", FieldType.STRING, outputTopics);
            metrics.setField("topic_offset_from", FieldType.LONG, fromOffset);
            metrics.setField("topic_offset_until", FieldType.LONG, untilOffset);
            metrics.setField("num_incoming_messages", FieldType.INT, untilOffset - fromOffset);
            metrics.setField("num_incoming_records", FieldType.INT, incomingEvents.size());
            metrics.setField("num_outgoing_records", FieldType.INT, outgoingEvents.size());

            float errorCount = outgoingEvents.stream().filter(r -> r.hasField(FieldDictionary.RECORD_ERRORS)).count();
            if (outgoingEvents.size() != 0)
                metrics.setField("error_percentage", FieldType.FLOAT, 100.0f * errorCount / outgoingEvents.size());




            final List<Integer> recordSizesInBytes = new ArrayList<>();
            final List<Integer> recordNumberOfFields = new ArrayList<>();

            outgoingEvents.forEach(record -> {
                recordSizesInBytes.add(record.sizeInBytes());
                recordNumberOfFields.add(record.size());
            });

            final int numberOfProcessedBytes = recordSizesInBytes.stream().mapToInt(Integer::intValue).sum();
            final int numberOfProcessedFields = recordNumberOfFields.stream().mapToInt(Integer::intValue).sum();

            if (numberOfProcessedFields != 0) {
                metrics.setField("average_bytes_per_field", INT, numberOfProcessedBytes / numberOfProcessedFields);
            }
            if (processingDurationInMillis != 0) {
                metrics.setField("average_bytes_per_second", INT, (int) (numberOfProcessedBytes * 1000 / processingDurationInMillis));
                metrics.setField("average_num_records_per_second", INT, (int) (outgoingEvents.size() * 1000 / processingDurationInMillis));
            }

            metrics.setField("average_fields_per_record", INT, numberOfProcessedFields / outgoingEvents.size());
            metrics.setField("average_bytes_per_record", INT, numberOfProcessedBytes / outgoingEvents.size());
            metrics.setField("total_bytes", INT, numberOfProcessedBytes);
            metrics.setField("total_fields", INT, numberOfProcessedFields);
            metrics.setField("total_processing_time_in_ms", LONG, processingDurationInMillis);

            metrics.setField(FieldDictionary.RECORD_TIME, LONG, new Date().getTime());
            return Collections.singleton(metrics);
        }


        return Collections.emptyList();
    }
}
