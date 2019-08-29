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
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"record", "fields", "remove", "delete"})
@CapabilityDescription("Keep only records based on a given field value or/and based on custom methods returning a boolean " +
        "(or something that can be casted into a boolean).")
@ExtraDetailFile("./details/common-processors/FilterRecords-Detail.rst")
@DynamicProperty(name = "Name of the method to add (the name is just for documentation purpose)",
        supportsExpressionLanguage = true,
        value = "an expression returning a boolean or an object that can be cast into a boolean that will be used to filter out records",
        description = "an expression returning a boolean or an object that can be cast into a boolean that will be used to filter out records",
        nameForDoc = "fakeMethod")
public class FilterRecords extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(FilterRecords.class);

    public static final PropertyDescriptor FIELD_NAME = new PropertyDescriptor.Builder()
            .name("field.name")
            .description("the field name")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELD_VALUE = new PropertyDescriptor.Builder()
            .name("field.value")
            .description("the field value to keep")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOGIC = new PropertyDescriptor.Builder()
            .name("logic")
            .description("the logic to use between the different filter criterias (methods).")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(new StandardValidators.EnumValidator(Logic.class))
            .allowableValues(Logic.values())
            .defaultValue(Logic.OR.getName().toUpperCase())
            .build();

    public static final PropertyDescriptor KEEP_ERRORS = new PropertyDescriptor.Builder()
            .name("keep.errors")
            .description("If you want to keep records that got exception while evaluating a custom method or not.")
            .required(false)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    private String fieldName;
    private String fieldValue;
    private Logic logic;
    private boolean keepErrors;
    private final Set<PropertyDescriptor> dynamicMethodProperties = new HashSet<>();

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        if (context.getPropertyValue(FIELD_NAME).isSet() && !context.getPropertyValue(FIELD_VALUE).isSet()) {
            validationResults.add(
                new ValidationResult.Builder()
                        .input(FIELD_VALUE.getName())
                        .explanation(String.format("%s must be set when %s is set",
                                FIELD_VALUE.getName(), FIELD_NAME.getName()
                        ))
                        .valid(false)
                        .build());
        }
        if (context.getPropertyValue(FIELD_VALUE).isSet() && !context.getPropertyValue(FIELD_NAME).isSet()) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input(FIELD_NAME.getName())
                            .explanation(String.format("%s must be set when %s is set",
                                    FIELD_NAME.getName(), FIELD_VALUE.getName()
                            ))
                            .valid(false)
                            .build());
        }
        if (!context.getPropertyValue(FIELD_VALUE).isSet() && noDynamicProps(context)) {
            validationResults.add(
                    new ValidationResult.Builder()
                            .input("Needs at least one criteria")
                            .explanation("Needs to set some criteria to filter records otherwise just delete the processor as it does nothing")
                            .valid(false)
                            .build());
        }
        return validationResults;
    }

    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        try {
            fieldName = context.getPropertyValue(FIELD_NAME).asString();
            fieldValue = context.getPropertyValue(FIELD_VALUE).asString();
            logic = Logic.valueOf(context.getPropertyValue(LOGIC).asString());
            keepErrors = context.getPropertyValue(KEEP_ERRORS).asBoolean();
            initDynamicProperties(context);
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        Stream<Record> filtringRecords = records.stream();
        if (fieldName != null && fieldValue != null) {
            filtringRecords = filtringRecords.filter(record -> record.hasField(fieldName) && record.getField(fieldName).asString().equals(fieldValue));
        }
        if (dynamicMethodProperties.isEmpty()) return filtringRecords.collect(Collectors.toList());
        switch (logic) {
            case AND:
                for (PropertyDescriptor filterMethodDescriptor: dynamicMethodProperties) {
                    filtringRecords = filtringRecords
                            .filter(r -> evaluateMethodDescriptor(context, r, filterMethodDescriptor));
                }
                break;
            case OR:
                filtringRecords = filtringRecords.filter(r -> {
                    for (PropertyDescriptor filterMethodDescriptor: dynamicMethodProperties) {
                        if (evaluateMethodDescriptor(context, r, filterMethodDescriptor)) return true;
                    }
                    return false;
                });
                break;
        }
        return filtringRecords.collect(Collectors.toList());
    }

    private boolean evaluateMethodDescriptor(ProcessContext context, Record record, PropertyDescriptor methodDescriptor) {
        try {
            return context.getPropertyValue(methodDescriptor).evaluate(record).asBoolean();
        } catch (Exception ex) {
            record.addError(ProcessError.EXPRESSION_LANGUAGE_EXECUTION_ERROR.getName(), ex.getMessage());
            return keepErrors;
        }
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELD_NAME);
        descriptors.add(FIELD_VALUE);
        descriptors.add(LOGIC);
        descriptors.add(KEEP_ERRORS);
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

    private void initDynamicProperties(ProcessContext context) {
        dynamicMethodProperties.clear();
        // loop over properties to cache dynamic ones
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            dynamicMethodProperties.add(entry.getKey());
        }
    }


    private boolean noDynamicProps(ValidationContext context) {
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            return false;
        }
        return true;
    }

    private enum Logic {

        AND,
        OR;

        public String toString() {
            return name;
        }
        private String name;

        Logic() {
            this.name = this.name().toLowerCase();
        }

        public String getName() {
            return name;
        }
    }
}
