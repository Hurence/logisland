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
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"record", "fields", "Add"})
@CapabilityDescription("Convert one or more field representing a date into a Unix Epoch Time (time in milliseconds since &st January 1970, 00:00:00 GMT)...")
@DynamicProperty(name = "field to convert",
        supportsExpressionLanguage = false,
        value = "a default value",
        description = "Add a field to the record with the default value")
public class ConvertSimpleDateFormatFields extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(ConvertSimpleDateFormatFields.class);


    private static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    private static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");


    private static final PropertyDescriptor TIMEZONE = new PropertyDescriptor.Builder()
            .name("timezone")
            .description("Specify the timezone (default is CET)")
            .required(true)
            .defaultValue("CET")
            .addValidator(StandardValidators.TIMEZONE_VALIDATOR)
            .build();

    private static final PropertyDescriptor INPUT_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("input.date.format")
            .description("Simple date format representation of the input field to convert")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CONFLICT_RESOLUTION_POLICY);
        descriptors.add(INPUT_DATE_FORMAT);
        descriptors.add(TIMEZONE);
        return Collections.unmodifiableList(descriptors);
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        SimpleDateFormat inputDateFormat = new SimpleDateFormat(context.getPropertyValue(INPUT_DATE_FORMAT).asString());
        TimeZone myTimeZone = TimeZone.getTimeZone(context.getPropertyValue(TIMEZONE).asString());
        inputDateFormat.setTimeZone(myTimeZone);

        Map<String, String> fieldsNameMapping = getFieldsNameMapping(context);
        for (Record record : records) {
            updateRecord(context, record, fieldsNameMapping, inputDateFormat, myTimeZone);
        }
        return records;
    }


    private void updateRecord(ProcessContext context, Record record, Map<String, String> fieldsNameMapping, SimpleDateFormat dateFormat, TimeZone tz) {

        String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();

        if ((fieldsNameMapping == null) || (fieldsNameMapping.keySet() == null)) {
            return;
        }
        fieldsNameMapping.keySet().forEach(addedFieldName -> {
            final String inputDateValue = context.getPropertyValue(addedFieldName).evaluate(record).asString();
            if (inputDateValue != null) {
                Date date = null;
                try {
                    date = dateFormat.parse(inputDateValue);
                } catch (ParseException e) {
                    // Left the record unchanged but log the failure/reason & cause
                    logger.info("Cannot parse input date: " + inputDateValue +
                            " - Date Format: " + context.getPropertyValue(INPUT_DATE_FORMAT) +
                            " - Reason: " + e.getMessage() +
                            " - Cause:" + e.getCause());
                } catch (Exception e) {
                    logger.info("Cannot parse input date: " + inputDateValue +
                            " - Date Format: " + context.getPropertyValue(INPUT_DATE_FORMAT) +
                            " - Reason: " + e.getMessage() +
                            " - Cause:" + e.getCause());
                }
                if (date != null) {
                    Long unixEpochTime = date.getTime();
                    // field is already here
                    if (record.hasField(addedFieldName)) {
                        if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                            overwriteObsoleteFieldValue(record, addedFieldName, unixEpochTime);
                        }
                    } else {
                        record.setField(addedFieldName, FieldType.LONG, unixEpochTime);
                    }
                }
            }});
    }

    private void overwriteObsoleteFieldValue(Record record, String fieldName, Long newValue) {
        final Field fieldToUpdate = record.getField(fieldName);
        record.removeField(fieldName);
        record.setField(fieldName, FieldType.LONG, newValue);
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
