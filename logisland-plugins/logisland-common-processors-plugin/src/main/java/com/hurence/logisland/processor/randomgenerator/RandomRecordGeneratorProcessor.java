package com.hurence.logisland.processor.randomgenerator;

import com.hurence.logisland.components.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.processor.AbstractRecordProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.serializer.AvroRecordSerializer;
import com.hurence.logisland.utils.avro.eventgenerator.DataGenerator;
import com.hurence.logisland.validators.StandardValidators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class RandomRecordGeneratorProcessor extends AbstractRecordProcessor {


    static final long serialVersionUID = -1L;

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


    private static Logger logger = LoggerFactory.getLogger(RandomRecordGeneratorProcessor.class);

    private static String EVENT_TYPE_NAME = "avro-generated-event";




    @Override
    public void init(final ProcessContext context) {


    }

    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {

        final String schemaContent = context.getProperty(INPUT_SCHEMA).asString();
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaContent);

        final DataGenerator dataGenerator = new DataGenerator(schemaContent);
        final RandomDataGenerator randomData = new RandomDataGenerator();
        final AvroRecordSerializer avroSerializer = new AvroRecordSerializer(schema);

        final int minEventsCount = context.getProperty(MIN_EVENTS_COUNT).asInteger();
        final int maxEventsCount = context.getProperty(MAX_EVENTS_COUNT).asInteger();
        final int eventsCount = randomData.nextInt(minEventsCount, maxEventsCount);
        logger.debug("generating {} events in [{},{}]", eventsCount, minEventsCount, maxEventsCount);

        List<Record> outRecords = new ArrayList<>();

        for (int i = 0; i < eventsCount; i++) {
            try {
                GenericRecord eventRecord = dataGenerator.generateRandomRecord();

                Record record = new Record(EVENT_TYPE_NAME);


                for (final Schema.Field schemaField : schema.getFields()) {

                    String fieldName = schemaField.name();
                    Object fieldValue = eventRecord.get(fieldName);
                    String fieldType = schemaField.schema().getType().getName();

                    if (Objects.equals(fieldName, "_id")) {
                        record.setId(fieldValue.toString());
                    } else if (!Objects.equals(fieldName, "_type")) {
                        if (fieldValue instanceof org.apache.avro.util.Utf8) {
                            record.setField(fieldName, fieldType, fieldValue.toString());
                        } else if (fieldValue instanceof GenericData.Array) {
                            GenericData.Array avroArray = (GenericData.Array) fieldValue;
                            List<Object> list = new ArrayList<>();
                            record.setField(fieldName, fieldType, list);
                            AvroRecordSerializer.copyArray(avroArray, list);
                        } else {
                            record.setField(fieldName, fieldType, fieldValue);
                        }

                    }
                }

                outRecords.add(record);
            } catch (Exception e) {
                logger.error("problem while generating random event from avro schema {}");
            }
        }


        return outRecords;
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
