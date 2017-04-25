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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.serializer.AvroSerializer;
import com.hurence.logisland.util.avro.eventgenerator.DataGenerator;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "avro", "generator"})
@CapabilityDescription("This is a processor that make random records given an Avro schema")
public class GenerateRandomRecord extends AbstractProcessor {


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

    public static final PropertyDescriptor OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("avro.output.schema")
            .description("the avro schema definition for the output serialization")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static Logger logger = LoggerFactory.getLogger(GenerateRandomRecord.class);

    private static String RECORD_TYPE = "random_record";


    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {

        final String schemaContent = context.getPropertyValue(OUTPUT_SCHEMA).asString();
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse(schemaContent);

        final DataGenerator dataGenerator = new DataGenerator(schemaContent);
        final RandomDataGenerator randomData = new RandomDataGenerator();
        final AvroSerializer avroSerializer = new AvroSerializer(schema);

        final int minEventsCount = context.getPropertyValue(MIN_EVENTS_COUNT).asInteger();
        final int maxEventsCount = context.getPropertyValue(MAX_EVENTS_COUNT).asInteger();
        final int eventsCount = randomData.nextInt(minEventsCount, maxEventsCount);
        logger.debug("generating {} events in [{},{}]", eventsCount, minEventsCount, maxEventsCount);

        List<Record> outRecords = new ArrayList<>();

        for (int i = 0; i < eventsCount; i++) {
            try {
                GenericRecord eventRecord = dataGenerator.generateRandomRecord();

                Record record = new StandardRecord(RECORD_TYPE);


                for (final Schema.Field schemaField : schema.getFields()) {

                    final String fieldName = schemaField.name();
                    final Object fieldValue = eventRecord.get(fieldName);
                    final FieldType fieldType;
                    {
                        /**
                         * currently all avro type are : RECORD,ENUM,ARRAY,MAP,UNION,FIXED,STRING,BYTES,INT,LONG,FLOAT,DOUBLE,BOOLEAN,NULL;
                         * all LogIsland type are : RECORD,ENUM,ARRAY,MAP,STRING,BYTES,INT,LONG,FLOAT,DOUBLE,BOOLEAN,NULL;
                         *
                         * So we lack of types: UNION, FIXED
                         */
                        Schema.Type avroType = schemaField.schema().getType();
                        switch (avroType) {
                            case UNION:
                                List<Schema> schemas = schemaField.schema().getTypes();
                                List<Schema.Type> types = new LinkedList<>();
                                for (Schema schemaF2 : schemas) {
                                    types.add(schemaF2.getType());
                                }

                                if (types.size() > 2 || !types.contains(Schema.Type.NULL)) {
                                    throw new UnsupportedOperationException(
                                            "avro schema with types UNION with another format than '['null', 'atype']' is not yet supported"
                                    );
                                }
                                Schema.Type determineType = null;
                                for (Schema.Type type : types) {
                                    switch (type) {
                                        case NULL:
                                            break;
                                        default:
                                            determineType = type;
                                            break;
                                    }
                                }
                                if (determineType == null) {
                                    throw new UnsupportedOperationException("UNION of null type makes no sense");
                                }
                                fieldType = FieldType.valueOf(determineType.getName().toUpperCase());
                                break;
                            case FIXED:
                                //TODO verify this is a correct behaviour
                                fieldType = FieldType.STRING;
                                break;
                            default:
                                fieldType = FieldType.valueOf(avroType.getName().toUpperCase());
                                break;
                        }
                    }

                    if (Objects.equals(fieldName, FieldDictionary.RECORD_ID)) {
                        record.setId(fieldValue.toString());
                    } else if (!Objects.equals(fieldName, FieldDictionary.RECORD_TYPE)) {
                        if (fieldValue instanceof org.apache.avro.util.Utf8) {
                            record.setStringField(fieldName, fieldValue.toString());
                        } else if (fieldValue instanceof GenericData.Array) {
                            GenericData.Array avroArray = (GenericData.Array) fieldValue;
                            List<Object> list = new ArrayList<>();
                            record.setField(fieldName, FieldType.ARRAY, list);
                            AvroSerializer.copyArray(avroArray, list);
                        } else {
                            record.setField(fieldName, fieldType, fieldValue);
                        }

                    }
                }

                outRecords.add(record);
            } catch (Exception e) {
                logger.error("problem while generating random event from avro schema {}", e);
            }
        }


        return outRecords;
    }


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OUTPUT_SCHEMA);
        descriptors.add(MIN_EVENTS_COUNT);
        descriptors.add(MAX_EVENTS_COUNT);

        return Collections.unmodifiableList(descriptors);
    }

}
