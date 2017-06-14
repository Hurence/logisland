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
import com.hurence.logisland.annotation.documentation.SeeAlso;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.util.time.DateUtil;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Tags({"parser", "regex", "log", "record"})
@CapabilityDescription("This processor is used to create a new set of fields from one field (using regexp).")
@SeeAlso(value = {RegexpProcessor.class}, classNames = {"com.hurence.logisland.processor.RegexpProcessor"})
@DynamicProperty(name = "alternative regex & mapping",
        supportsExpressionLanguage = true,
        value = "another regex that could match",
        description = "This processor is used to create a new set of fields from one field (using regexp).")
public class RegexpProcessor extends AbstractProcessor {

    static final long serialVersionUID = 1413578915552852739L;

    private static Logger logger = LoggerFactory.getLogger(RegexpProcessor.class);

    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    public static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field", "keep only old field");


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
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        Map<String, regexRule> fieldsNameMapping = getFieldsNameMapping(context);

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
                        if (mappingRule.getValueRegex() != null){
                            // Apply the regex to the value
                            String fieldValue = record.getField(sourceAttr).asString();
                            try {
                                Pattern valueRegex = mappingRule.getValueRegex();
                                Matcher valueMatcher = valueRegex.matcher(fieldValue);
                                if (valueMatcher.lookingAt()){
                                    extractValueFields(mappingRule.getFieldsToProduce(), record, valueMatcher, context);
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


    private void extractValueFields(String[] valueFields, Record outputRecord, Matcher valueMatcher, ProcessContext context) {
        String conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();

        for (int i = 0; i < Math.min(valueMatcher.groupCount() + 1, valueFields.length); i++) {
            String content = valueMatcher.group(i + 1);
            String fieldName = valueFields[i];
            if (content != null) {
                if (outputRecord.hasField(fieldName) &&
                        (outputRecord.getField(fieldName).asString() != null) &&
                        (! outputRecord.getField(fieldName).asString().isEmpty())) {
                    if (conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                        outputRecord.setStringField(fieldName, content.replaceAll("\"", ""));
                    }
                }
                else {
                    outputRecord.setStringField(fieldName, content.replaceAll("\"", ""));
                }
            }
        }
    }

    private class regexRule {
        private String[] fieldsToProduce;
        private Pattern valueRegex = null;

        public String[] getFieldsToProduce() {
            return fieldsToProduce;
        }

        public Pattern getValueRegex() {
            return valueRegex;
        }

        public regexRule(String[] fields, String r){
            fieldsToProduce = fields;
            if (r != null) {
                valueRegex = Pattern.compile(r);
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
                String[] fieldsToProduce = splitedElements[0].split(",");
                String regexp = splitedElements[1];
                fieldsNameMappings.put(fieldName, new regexRule(fieldsToProduce, regexp));
            }
            else {
                // Ignore the mapping rule
                logger.warn("The mapping rule for {} has the invalid value: {}", entry.getKey().getName(), entry.getValue());
            }
        }
        return fieldsNameMappings;
    }

}
