package com.hurence.logisland.utils.event;

import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hurence.logisland.record.FieldType.*;

/**
 * Created by tom on 09/09/16.
 */
public class ProcessorMetrics {

    public static String METRICS_EVENT_TYPE = "logisland_metrics";

    private static Logger logger = LoggerFactory.getLogger(ProcessorMetrics.class);

    /**
     * compute roughly the size in bytes for an event
     * id, type and creationDate are ignored
     *
     * @param record
     * @return
     */
    public synchronized static int getEventSizeInBytes(final Record record) {

        int size = 0;

        for (Map.Entry<String, Field> entry : record.getFieldsEntrySet()) {

            Field field = entry.getValue();
            Object fieldValue = field.getRawValue();
            FieldType fieldType = field.getType();

            // dump event field as record attribute

            try {
                switch (fieldType) {
                    case STRING:
                        size += ((String) fieldValue).getBytes().length;
                        break;
                    case INT:
                        size += 4;
                        break;
                    case LONG:
                        size += 8;
                        break;
                    case FLOAT:
                        size += 4;
                        break;
                    case DOUBLE:
                        size += 8;
                        break;
                    case BOOLEAN:
                        size += 1;
                        break;
                    default:
                        break;
                }
            } catch (Exception ex) {
                logger.debug("exception while sizing field {}", field);
            }

        }

        return size;
    }

    public synchronized static Collection<Record> computeMetrics(final Collection<Record> records,
                                                                 final Map<String, Field> processorFields,
                                                                 final long processingDurationInMillis) {


        if (records.size() != 0) {
            Record metrics = new Record(METRICS_EVENT_TYPE);

            metrics.setFields(processorFields);
            metrics.setStringField("search_index", METRICS_EVENT_TYPE);

            final List<Integer> eventSizesInBytes = new ArrayList<>();
            final List<Integer> eventNumberOfFields = new ArrayList<>();

            records.stream().forEach(event -> {
                eventSizesInBytes.add(getEventSizeInBytes(event));
                eventNumberOfFields.add(event.getFieldsEntrySet().size());
            });

            final int numberOfProcessedBytes = eventSizesInBytes.stream().mapToInt(Integer::intValue).sum();
            final int numberOfProcessedFields = eventNumberOfFields.stream().mapToInt(Integer::intValue).sum();

            metrics.setField("total_bytes", INT, numberOfProcessedBytes);
            metrics.setField("total_fields", INT, numberOfProcessedFields);
            metrics.setField("average_fields_per_event", INT, numberOfProcessedFields / records.size());
            metrics.setField("average_bytes_per_event", INT, numberOfProcessedBytes / records.size());
            metrics.setField("average_time_per_event", INT, (int) ( processingDurationInMillis / records.size() ));
            metrics.setField("total_time", LONG, processingDurationInMillis);

            if (numberOfProcessedFields != 0) {
                metrics.setField("average_bytes_per_field", INT, numberOfProcessedBytes / numberOfProcessedFields);
            }
            if (numberOfProcessedFields != 0) {
                metrics.setField("average_time_per_field", INT, (int) (processingDurationInMillis / numberOfProcessedFields));
            }
            if (processingDurationInMillis != 0) {
                metrics.setField("average_bytes_per_second", INT, (int) (numberOfProcessedBytes * 1000 / processingDurationInMillis));
                metrics.setField("average_events_per_second", INT, (int) (records.size() * 1000 / processingDurationInMillis));

            }

            metrics.setField("event_time", LONG, new Date().getTime());
            return Collections.singleton(metrics);
        }


        return Collections.emptyList();
    }
}
