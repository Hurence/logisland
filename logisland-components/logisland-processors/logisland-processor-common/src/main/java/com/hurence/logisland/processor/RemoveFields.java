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

import com.google.common.collect.Lists;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"record", "fields", "remove", "delete", "keep"})
@CapabilityDescription("Removes a list of fields defined by a comma separated list of field names or keeps only fields " +
        "defined by a comma separated list of field names.")
public class RemoveFields extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    public static final String KEY_FIELDS_TO_REMOVE = "fields.to.remove";
    public static final String KEY_FIELDS_TO_KEEP = "fields.to.keep";

    private static final Logger logger = LoggerFactory.getLogger(RemoveFields.class);

    public static final PropertyDescriptor FIELDS_TO_REMOVE = new PropertyDescriptor.Builder()
            .name(KEY_FIELDS_TO_REMOVE)
            .description("A comma separated list of field names to remove (e.g. 'policyid,date_raw'). Usage of this " +
                    "property is mutually exclusive with the " + KEY_FIELDS_TO_KEEP + " property. In any case the " +
                    "technical logisland fields " + FieldDictionary.RECORD_ID + ", " + FieldDictionary.RECORD_TIME +
                    " and " + FieldDictionary.RECORD_TYPE + " are not removed even if specified in the list to " +
                    "remove.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS_TO_KEEP = new PropertyDescriptor.Builder()
            .name(KEY_FIELDS_TO_KEEP)
            .description("A comma separated list of field names to keep (e.g. 'policyid,date_raw'. All other fields " +
                    "will be removed. Usage of this property is mutually exclusive with the " + FIELDS_TO_REMOVE +
                    " property. In any case the technical logisland fields " + FieldDictionary.RECORD_ID +
                    ", " + FieldDictionary.RECORD_TIME + " and " + FieldDictionary.RECORD_TYPE + " are not removed " +
                    "even if not specified in the list to keep.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    // Remove or keep mode
    private boolean remove;
    private List<String> fieldsToRemove;
    List<String> fieldsToKeep;

    @Override
    public void init(ProcessContext context) {
        super.init(context);
        PropertyValue propertyValue = context.getPropertyValue(FIELDS_TO_REMOVE);

        if (propertyValue.asString() == null)
        {
            remove = false;
            propertyValue = context.getPropertyValue(FIELDS_TO_KEEP);
            fieldsToKeep = Lists.newArrayList(propertyValue.asString().split(","));
        } else
        {
            remove = true;
            fieldsToRemove = Lists.newArrayList(propertyValue.asString().split(","));
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        try {
            for (Record record : records) {
                new ArrayList<>(record.getAllFields()).forEach(field -> {
                    String fieldName = field.getName();
                    if (remove) {
                        // Remove mode
                        if (fieldsToRemove.contains(fieldName)) {
                            record.removeField(fieldName);
                        }
                    }
                    else
                    {
                        if (!fieldName.equals(FieldDictionary.RECORD_ID)
                                && !fieldName.equals(FieldDictionary.RECORD_TIME)
                                && !fieldName.equals(FieldDictionary.RECORD_TYPE)) {
                            // Keep mode
                            if (!fieldsToKeep.contains(fieldName)) {
                                record.removeField(fieldName);
                            }
                        }
                    }
                });
            }
        } catch (Exception ex) {
            if (remove) {
                logger.warn("issue while trying to remove field list {} :  {}",
                        context.getPropertyValue(FIELDS_TO_REMOVE).asString(),
                        ex.toString());
            }
            else
            {
                logger.warn("issue while trying to handle keep field list {} :  {}",
                        context.getPropertyValue(FIELDS_TO_KEEP).asString(),
                        ex.toString());
            }
        }

        return records;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(
                Lists.newArrayList(FIELDS_TO_REMOVE, FIELDS_TO_KEEP));
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        /**
         * Only one of both properties may be set.
         */

        // Be sure not both are defined
        if (context.getPropertyValue(FIELDS_TO_REMOVE).isSet() && context.getPropertyValue(FIELDS_TO_KEEP).isSet())
        {
            validationResults.add(
                    new ValidationResult.Builder()
                            .explanation(FIELDS_TO_REMOVE.getName() + " and " + FIELDS_TO_KEEP.getName() +
                                    " properties are mutually exclusive.")
                            .valid(false)
                            .build());
        }

        // Be sure at least one is defined
        if (!context.getPropertyValue(FIELDS_TO_REMOVE).isSet() && !context.getPropertyValue(FIELDS_TO_KEEP).isSet())
        {
            validationResults.add(
                    new ValidationResult.Builder()
                            .explanation("One of both properties " + FIELDS_TO_REMOVE.getName() + " or " +
                                    FIELDS_TO_KEEP.getName() + " should be defined.")
                            .valid(false)
                            .build());
        }

        /**
         * No empty values
         */

        if (context.getPropertyValue(FIELDS_TO_REMOVE).isSet())
        {
            if (context.getPropertyValue(FIELDS_TO_REMOVE).asString().trim().length() == 0) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .explanation(FIELDS_TO_REMOVE.getName() + " cannot be empty if set.")
                                .valid(false)
                                .build());
            }
        }

        if (context.getPropertyValue(FIELDS_TO_KEEP).isSet())
        {
            if (context.getPropertyValue(FIELDS_TO_KEEP).asString().trim().length() == 0) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .explanation(FIELDS_TO_KEEP.getName() + " cannot be empty if set.")
                                .valid(false)
                                .build());
            }
        }

        return validationResults;
    }
}
