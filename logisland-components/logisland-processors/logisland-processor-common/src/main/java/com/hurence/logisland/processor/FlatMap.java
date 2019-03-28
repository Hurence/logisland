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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;
import java.util.stream.Collectors;

@Tags({"record", "fields", "flatmap", "flatten"})
@CapabilityDescription("Converts each field records into a single flatten record...")
public class FlatMap extends AbstractProcessor {


    public static final PropertyDescriptor KEEP_ROOT_RECORD = new PropertyDescriptor.Builder()
            .name("keep.root.record")
            .description("do we add the original record in")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final PropertyDescriptor INCLUDE_POSITION = new PropertyDescriptor.Builder()
            .name("include.position")
            .description("do we add the original record position in")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONCAT_FIELDS = new PropertyDescriptor.Builder()
            .name("concat.fields")
            .description("comma separated list of fields to apply concatenation ex : $rootField/$leaffield")
            .required(false)
            .build();

    public static final PropertyDescriptor CONCAT_SEPARATOR = new PropertyDescriptor.Builder()
            .name("concat.separator")
            .description("returns $rootField/$leaf/field")
            .required(false)
            .defaultValue("/")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor COPY_ROOT_RECORD_FIELDS = new PropertyDescriptor.Builder()
            .name("copy.root.record.fields")
            .description("do we copy the original record fields into the flattened records")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final PropertyDescriptor LEAF_RECORD_TYPE = new PropertyDescriptor.Builder()
            .name("leaf.record.type")
            .description("the new type for the flattened records if present")
            .required(false)
            .defaultValue("")
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEEP_ROOT_RECORD);
        descriptors.add(COPY_ROOT_RECORD_FIELDS);
        descriptors.add(LEAF_RECORD_TYPE);
        descriptors.add(CONCAT_FIELDS);
        descriptors.add(CONCAT_SEPARATOR);
        descriptors.add(INCLUDE_POSITION);

        return descriptors;
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String leafRecordType = context.getPropertyValue(LEAF_RECORD_TYPE).asString();
        boolean addRootRecord = context.getPropertyValue(KEEP_ROOT_RECORD).asBoolean();
        boolean includePosition = context.getPropertyValue(INCLUDE_POSITION).asBoolean();
        List<String> concatFields = context.getPropertyValue(CONCAT_FIELDS).isSet() ?
                Arrays.asList(context.getPropertyValue(CONCAT_FIELDS).asString().split(",")) :
                Collections.emptyList();
        String concatSeparator = context.getPropertyValue(CONCAT_SEPARATOR).asString();
        boolean copyRootRecordFields = context.getPropertyValue(COPY_ROOT_RECORD_FIELDS).asBoolean();
        List<Record> outputRecords = new ArrayList<>();

        records.forEach(rootRecord -> {

            // do we keep original ?
            if (addRootRecord)
                outputRecords.add(rootRecord);

            // separate leaf from root fields
            List<Field> leafFields = rootRecord.getAllFields().stream()
                    .filter(field -> field.getType().equals(FieldType.RECORD) &&
                            !field.getName().equals(FieldDictionary.RECORD_TYPE) &&
                            !field.getName().equals(FieldDictionary.RECORD_ID) &&
                            !field.getName().equals(FieldDictionary.RECORD_TIME) &&
                            !field.getName().equals(FieldDictionary.RECORD_POSITION))
                    .collect(Collectors.toList());

            List<Field> rootFields = rootRecord.getAllFields().stream()
                    .filter(field -> !field.getType().equals(FieldType.RECORD))
                    .collect(Collectors.toList());

            // convert each field of type record into a new record
            leafFields.forEach(field -> {

                Record flattenRecord = field.asRecord();

                // change type if requested
                if (!leafRecordType.isEmpty())
                    flattenRecord.setType(leafRecordType);

                // denormalize leaf with root values except id and time
                if (copyRootRecordFields) {
                    if (concatFields.isEmpty()) {
                        rootFields.forEach(flattenRecord::setField);
                    } else {
                        rootFields.forEach(rootField -> {

                            String concatFieldName = rootField.getName();
                            // do concat if needed
                            if (concatFields.contains(concatFieldName) &&
                                    rootField.getType() == FieldType.STRING &&
                                    flattenRecord.hasField(concatFieldName) &&
                                    flattenRecord.getField(concatFieldName).getType() == FieldType.STRING) {


                                flattenRecord.setStringField(concatFieldName,
                                        rootField.asString() + concatSeparator +
                                                flattenRecord.getField(rootField.getName()).asString());
                            }else {
                                flattenRecord.setField(rootField);
                            }
                        });

                    }


                    // denormalize root record position
                    if(includePosition && rootRecord.hasPosition()){
                        Position position = rootRecord.getPosition();

                        flattenRecord.setField(FieldDictionary.RECORD_POSITION_LATITUDE, FieldType.DOUBLE, position.getLatitude())
                                .setField(FieldDictionary.RECORD_POSITION_LONGITUDE, FieldType.DOUBLE, position.getLongitude())
                                .setField(FieldDictionary.RECORD_POSITION_ALTITUDE, FieldType.DOUBLE, position.getAltitude())
                                .setField(FieldDictionary.RECORD_POSITION_HEADING, FieldType.DOUBLE, position.getHeading())
                                .setField(FieldDictionary.RECORD_POSITION_PRECISION, FieldType.DOUBLE, position.getPrecision())
                                .setField(FieldDictionary.RECORD_POSITION_SATELLITES, FieldType.INT, position.getSatellites())
                                .setField(FieldDictionary.RECORD_POSITION_SPEED, FieldType.DOUBLE, position.getSpeed())
                                .setField(FieldDictionary.RECORD_POSITION_STATUS, FieldType.INT, position.getStatus())
                                .setField(FieldDictionary.RECORD_POSITION_TIMESTAMP, FieldType.LONG, position.getTimestamp().getTime());

                    }
                }

                outputRecords.add(flattenRecord);

            });
        });


        return outputRecords;
    }


}
