package com.hurence.logisland.utils.event;

import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
     * @param event
     * @return
     */
    public synchronized static int getEventSizeInBytes(final Event event) {

        int size = 0;

        for (Map.Entry<String, EventField> entry : event.entrySet()) {

            EventField field = entry.getValue();
            Object fieldValue = field.getValue();
            String fieldType = field.getType();

            // dump event field as record attribute

            try {
                switch (fieldType.toLowerCase()) {
                    case "string":
                        size += ((String) fieldValue).getBytes().length;
                        break;
                    case "short":
                        size += 2;
                        break;
                    case "integer":
                        size += 4;
                        break;
                    case "long":
                        size += 8;
                        break;
                    case "float":
                        size += 4;
                        break;
                    case "double":
                        size += 8;
                        break;
                    case "boolean":
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

    public synchronized static Collection<Event> computeMetrics(final Collection<Event> events,
                            final Map<String, EventField> processorFields,
                            final long processingDurationInMillis) {


        if (events.size() != 0) {
            Event metrics = new Event(METRICS_EVENT_TYPE);

            metrics.setFields(processorFields);
            metrics.put("search_index", "string", METRICS_EVENT_TYPE);

            final List<Integer> eventSizesInBytes = new ArrayList<>();
            final List<Integer> eventNumberOfFields = new ArrayList<>();

            events.stream().forEach(event -> {
                eventSizesInBytes.add(getEventSizeInBytes(event));
                eventNumberOfFields.add(event.entrySet().size());
            });

            final int numberOfProcessedBytes = eventSizesInBytes.stream().mapToInt(Integer::intValue).sum();
            final int numberOfProcessedFields = eventNumberOfFields.stream().mapToInt(Integer::intValue).sum();

            metrics.put("total_bytes", "int", numberOfProcessedBytes);
            metrics.put("total_fields", "int", numberOfProcessedFields);
            metrics.put("average_fields_per_event", "int", numberOfProcessedFields / events.size());
            metrics.put("average_bytes_per_event", "int", numberOfProcessedBytes / events.size());
            metrics.put("average_time_per_event", "int", (int) ( processingDurationInMillis /events.size() ));
            metrics.put("total_time", "long", processingDurationInMillis);

            if (numberOfProcessedFields != 0) {
                metrics.put("average_bytes_per_field", "int", numberOfProcessedBytes / numberOfProcessedFields);
            }
            if (numberOfProcessedFields != 0) {
                metrics.put("average_time_per_field", "int", (int) (processingDurationInMillis / numberOfProcessedFields));
            }
            if (processingDurationInMillis != 0) {
                metrics.put("average_bytes_per_second", "int", (int) (numberOfProcessedBytes * 1000 / processingDurationInMillis));
                metrics.put("average_events_per_second", "int", (int) (events.size() * 1000 / processingDurationInMillis));

            }

            metrics.put("event_time", "long", new Date().getTime());
            return Collections.singleton(metrics);
        }


        return Collections.emptyList();
    }
}
