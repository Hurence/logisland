package com.hurence.logisland.processor.randomgenerator;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.event.Event;
import com.hurence.logisland.processor.AbstractEventProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.serializer.EventAvroSerializer;
import com.hurence.logisland.utils.avro.eventgenerator.DataGenerator;
import com.hurence.logisland.validators.StandardValidators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class RandomEventGeneratorProcessor extends AbstractEventProcessor {

    public static final PropertyDescriptor MIN_EVENTS_COUNT = new PropertyDescriptor.Builder()
            .name("min.events.count")
            .description("the minimum number of generated events each run")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor MAX_EVENTS_COUNT = new PropertyDescriptor.Builder()
            .name("max.events.count")
            .description("the maximum number of generated events each run")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("200")
            .build();


    private static Logger logger = LoggerFactory.getLogger(RandomEventGeneratorProcessor.class);

    private static String EVENT_TYPE_NAME = "avro-generated-event";




    @Override
    public void init(final ProcessContext context) {


    }

    @Override
    public Collection<Event> process(final ProcessContext context, final Collection<Event> collection) {

        final String schemaContent = context.getProperty(INPUT_SCHEMA).getValue();
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaContent);

        final DataGenerator dataGenerator = new DataGenerator(schemaContent);
        final RandomDataGenerator randomData = new RandomDataGenerator();
        final EventAvroSerializer avroSerializer = new EventAvroSerializer(schema);

        final int minEventsCount = context.getProperty(MIN_EVENTS_COUNT).asInteger();
        final int maxEventsCount = context.getProperty(MAX_EVENTS_COUNT).asInteger();
        final int eventsCount = randomData.nextInt(minEventsCount, maxEventsCount);
        logger.debug("generating {} events in [{},{}]", eventsCount, minEventsCount, maxEventsCount);

        List<Event> outEvents = new ArrayList<>();

        for (int i = 0; i < eventsCount; i++) {
            try {
                GenericRecord eventRecord = dataGenerator.generateRandomRecord();

                Event event = new Event(EVENT_TYPE_NAME);


                for (final Schema.Field schemaField : schema.getFields()) {

                    String fieldName = schemaField.name();
                    Object fieldValue = eventRecord.get(fieldName);
                    String fieldType = schemaField.schema().getType().getName();

                    if (Objects.equals(fieldName, "_id")) {
                        event.setId(fieldValue.toString());
                    } else if (!Objects.equals(fieldName, "_type")) {
                        if (fieldValue instanceof org.apache.avro.util.Utf8) {
                            event.put(fieldName, fieldType, fieldValue.toString());
                        } else if (fieldValue instanceof GenericData.Array) {
                            GenericData.Array avroArray = (GenericData.Array) fieldValue;
                            List<Object> list = new ArrayList<>();
                            event.put(fieldName, fieldType, list);
                            EventAvroSerializer.copyArray(avroArray, list);
                        } else {
                            event.put(fieldName, fieldType, fieldValue);
                        }

                    }
                }

                outEvents.add(event);
            } catch (Exception e) {
                logger.error("problem while generating random event from avro schema {}");
            }
        }


        return outEvents;
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INPUT_TOPICS);
        descriptors.add(OUTPUT_TOPICS);
        descriptors.add(INPUT_SCHEMA);
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(MIN_EVENTS_COUNT);
        descriptors.add(MAX_EVENTS_COUNT);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
