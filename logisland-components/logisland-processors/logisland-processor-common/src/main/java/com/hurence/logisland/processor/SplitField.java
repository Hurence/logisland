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
import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;


@Category(ComponentCategory.PARSING)
@Tags({"parser", "split", "log", "record"})
@CapabilityDescription("This processor is used to create a new set of fields from one field (using split).")
@SeeAlso(value = {SplitField.class}, classNames = {"com.hurence.logisland.processor.SplitField"})
@DynamicProperty(name = "alternative split field",
        supportsExpressionLanguage = true,
        value = "another split that could match",
        description = "This processor is used to create a new set of fields from one field (using split).")
@ExtraDetailFile("./details/common-processors/SplitField-Detail.rst")
public class SplitField extends AbstractProcessor {

    private Map<String, regexRule> fieldsNameMapping;
    private String splitCounterSuffix;
    private boolean isEnabledSplitCounter;
    private int nbSplitLimit;

    static final long serialVersionUID = 1413578915552852739L;

    private static Logger logger = LoggerFactory.getLogger(SplitField.class);

    public static final PropertyDescriptor NB_SPLIT_LIMIT = new PropertyDescriptor.Builder()
            .name("split.limit")
            .description("Specify the maximum number of split to allow")
            .required(false)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    public static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field", "keep only old field");

    public static final PropertyDescriptor ENABLE_SPLIT_COUNTER = new PropertyDescriptor.Builder()
            .name("split.counter.enable")
            .description("Enable the counter of items returned by the split")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPLIT_COUNTER_SUFFIX = new PropertyDescriptor.Builder()
            .name("split.counter.suffix")
            .description("Enable the counter of items returned by the split")
            .required(false)
            .defaultValue("Counter")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
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
        descriptors.add(NB_SPLIT_LIMIT);
        descriptors.add(ENABLE_SPLIT_COUNTER);
        descriptors.add(SPLIT_COUNTER_SUFFIX);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException{
        super.init(context);
        this.fieldsNameMapping = getFieldsNameMapping(context);
        this.nbSplitLimit = context.getPropertyValue(NB_SPLIT_LIMIT).asInteger();
        this.isEnabledSplitCounter = context.getPropertyValue(ENABLE_SPLIT_COUNTER).asBoolean();
        this.splitCounterSuffix = context.getPropertyValue(SPLIT_COUNTER_SUFFIX).asString();
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        if ((fieldsNameMapping == null) || fieldsNameMapping.isEmpty()){
            return records;
        }

        /**
         * try to match the regexp to create an event
         */
        records.forEach(record -> {
            try {
                for (String sourceAttr : fieldsNameMapping.keySet()){
                    if (record.hasField(sourceAttr) &&
                            (record.getField(sourceAttr).asString() != null) &&
                            (! record.getField(sourceAttr).asString().isEmpty())){
                        regexRule mappingRule = fieldsNameMapping.get(sourceAttr);
                        if (mappingRule.getSplitRegexp() != null){
                            // Apply the regex to the value
                            String fieldValue = record.getField(sourceAttr).asString();
                            try {
                                String splitR = mappingRule.getSplitRegexp();
                                String[] values = fieldValue.split(splitR, nbSplitLimit);
                                if (values != null) {
                                    // Add the array of values to the new field to create.
                                    extractValueFields(mappingRule.getFieldToProduce(), record, values, context);
                                }
                            }
                            catch(Exception e){
                                logger.warn("issue while matching on record {}, exception {}", record, e.getMessage());
                            }

                        }
                        else {
                            // There is no defined regexp.
                            // Simply ignore the mapping rule
                            continue;
                        }
                    }
                }
            } catch (Exception e) {
                // nothing to do here
                logger.warn("issue while matching getting K/V on record {}, exception {}", record, e.getMessage());
            }
        });

        return records;
    }


    private void extractValueFields(String valueField, Record outputRecord, String[] values, ProcessContext context) {
        String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
        String fieldName = valueField;

        if (outputRecord.hasField(fieldName) &&
                (outputRecord.getField(fieldName).asString() != null) &&
                (! outputRecord.getField(fieldName).asString().isEmpty())) {
            if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                //outputRecord.setStringField(fieldName, content.replaceAll("\"", ""));
                outputRecord.setField(fieldName, FieldType.ARRAY, values);
                if (this.isEnabledSplitCounter){
                    outputRecord.setField(fieldName+this.splitCounterSuffix, FieldType.INT, values.length);
                }
            }
        }
        else {
            outputRecord.setField(fieldName, FieldType.ARRAY, values);
            //outputRecord.setStringField(fieldName, content.replaceAll("\"", ""));
            if (this.isEnabledSplitCounter){
                outputRecord.setField(fieldName+this.splitCounterSuffix, FieldType.INT, values.length);
            }
        }
    }

    private class regexRule {
        private String fieldToProduce;
        private String splitRegexp = null;

        public String getSplitRegexp() {return splitRegexp;}

        public String getFieldToProduce() {
            return fieldToProduce;
        }

        public regexRule(String field, String r){
            fieldToProduce = field;
            if (r != null) {
                splitRegexp = r;
                // Check the given expression to use in split is correct.
                // Raise a PatternSyntaxException if incorrect.
                Pattern.compile(r);
            }
        }
    }

    /*
     * Build a map of mapping rules of the form:
     *
     */
    private Map<String, regexRule> getFieldsNameMapping(ProcessContext context) {
        /**
         * list alternative regex
         */
        Map<String, regexRule> fieldsNameMappings = new HashMap<>();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }

            final String fieldName = entry.getKey().getName();
            final String[] splitedElements = entry.getValue().split(":", 2);
            if (splitedElements.length == 2) {
                String fieldToProduce = splitedElements[0];
                String regexp = splitedElements[1];
                fieldsNameMappings.put(fieldName, new regexRule(fieldToProduce, regexp));
            }
            else {
                // Ignore the mapping rule
                logger.warn("The mapping rule for {} has the invalid value: {}", entry.getKey().getName(), entry.getValue());
            }
        }
        return fieldsNameMappings;
    }

}
