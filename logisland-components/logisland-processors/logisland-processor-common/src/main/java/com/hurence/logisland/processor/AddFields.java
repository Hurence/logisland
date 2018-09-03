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

import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"record", "fields", "Add"})
@CapabilityDescription("Add one or more field with a default value\n" +
        "...")
@DynamicProperty(name = "field to add",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public class AddFields extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(AddFields.class);


    private static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    private static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");


    private static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(CONFLICT_RESOLUTION_POLICY);
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        Map<String, String> fieldsNameMapping = getFieldsNameMapping(context);
        for (Record record : records) {
            updateRecord(context, record, fieldsNameMapping);
        }
        return records;
    }


    private void updateRecord(ProcessContext context, Record record, Map<String, String> fieldsNameMapping) {

        String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();

        if ((fieldsNameMapping == null) || (fieldsNameMapping.keySet() == null)) {
            return;
        }
        fieldsNameMapping.keySet().forEach(addedFieldName -> {
            final String defaultValueToAdd = context.getPropertyValue(addedFieldName).evaluate(record).asString();
            // field is already here
            if (record.hasField(addedFieldName)) {
                if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                    overwriteObsoleteFieldValue(record, addedFieldName, defaultValueToAdd);
                }
            } else {
                record.setStringField(addedFieldName, defaultValueToAdd);
            }
        });
    }

    private void overwriteObsoleteFieldValue(Record record, String fieldName, String newValue) {
        final Field fieldToUpdate = record.getField(fieldName);
        record.removeField(fieldName);
        record.setField(fieldName, fieldToUpdate.getType(), newValue);
    }

    private Map<String, String> getFieldsNameMapping(ProcessContext context) {
        /**
         * list alternative regex
         */
        Map<String, String> fieldsNameMappings = new HashMap<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String fieldName = entry.getKey().getName();
            final String mapping = entry.getValue();

            fieldsNameMappings.put(fieldName, mapping);
        }
        return fieldsNameMappings;
    }
}
