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
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Tags({"record", "fields", "flatmap", "flatten"})
@CapabilityDescription("Converts each field records into a single flatten record\n" +
        "...")
public class FlatMap extends AbstractProcessor {


    public static final PropertyDescriptor KEEP_ROOT_RECORD = new PropertyDescriptor.Builder()
            .name("keep.root.record")
            .description("do we add the original record in")
            .required(false)
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
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

        return descriptors;
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String leafRecordType = context.getPropertyValue(LEAF_RECORD_TYPE).asString();
        boolean addRootRecord = context.getPropertyValue(KEEP_ROOT_RECORD).asBoolean();
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
                            !field.getName().equals(FieldDictionary.RECORD_TIME))
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
                    rootFields.forEach(flattenRecord::setField);
                }

                outputRecords.add(flattenRecord);

            });
        });


        return outputRecords;
    }


}