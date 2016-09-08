package com.hurence.logisland.processor.debug;

import com.hurence.logisland.components.AllowableValue;
import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.event.EventField;
import com.hurence.logisland.processor.AbstractEventProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.serializer.EventJsonSerializer;
import com.hurence.logisland.serializer.EventSerializer;
import com.hurence.logisland.validators.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


public class EventDebuggerProcessor extends AbstractEventProcessor {

    private static Logger logger = LoggerFactory.getLogger(EventDebuggerProcessor.class);


    public static final AllowableValue VERBOSE = new AllowableValue("verbose", "Maximum output",
            "logs every events");

    public static final AllowableValue EVENT_STATS = new AllowableValue("event", "Only statistics",
            "logs only stats");

    public static final AllowableValue COLLECTION_STATS = new AllowableValue("collection", "Only statistics for event collection",
            "logs only global stats");

    public static final PropertyDescriptor DEBUG_LEVEL = new PropertyDescriptor.Builder()
            .name("debug.level")
            .description("the granularity of debug output")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(EVENT_STATS.getValue())
            .allowableValues(EVENT_STATS, COLLECTION_STATS, VERBOSE)
            .build();


    /**
     * compute roughly the size in bytes for an event
     * id, type and creationDate are ignored
     *
     * @param event
     * @return
     */
    int getEventSizeInBytes(final Event event) {

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

    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> collection) {
        if (collection.size() != 0) {
            final EventSerializer serializer = new EventJsonSerializer();
            final List<Integer> eventSizesInBytes = new ArrayList<>();


            collection.stream().forEach(event -> {
                final int eventSizeInBytes = getEventSizeInBytes(event);
                eventSizesInBytes.add(eventSizeInBytes);
                StringBuilder logOutput = new StringBuilder();

                if (context.getProperty(DEBUG_LEVEL).getValue().equals(VERBOSE.getValue())) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    serializer.serialize(baos, event);
                    try {
                        baos.close();
                    } catch (IOException e) {
                        logger.debug("error {} ", e.getCause());
                    }

                    logOutput.append("processing : ")
                            .append(Arrays.toString(baos.toByteArray()))
                            .append("\n");

                }
                if (!context.getProperty(DEBUG_LEVEL).getValue().equals(COLLECTION_STATS.getValue())) {
                    logOutput.append("event size : ")
                            .append(eventSizeInBytes)
                            .append(" bytes for ")
                            .append(event.entrySet().size())
                            .append(" fields");

                    logger.info("{}", logOutput);
                }

            });

            final int totalSizeInBytes = eventSizesInBytes.stream().mapToInt(Integer::intValue).sum();
            logger.info("event collection contains {} events for a size of {} bytes", collection.size(), totalSizeInBytes);

        }


        return Collections.emptyList();
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ERROR_TOPICS);
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(INPUT_SCHEMA);
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(DEBUG_LEVEL);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
