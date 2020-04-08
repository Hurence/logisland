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

import com.hurence.logisland.annotation.behavior.DynamicProperties;
import com.hurence.logisland.annotation.behavior.DynamicProperty;
import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Category(ComponentCategory.PROCESSING)
@Tags({"record", "fields", "Add"})
@CapabilityDescription("Add one or more field to records")
@ExtraDetailFile("./details/common-processors/AddFields-Detail.rst")
@DynamicProperties(value = {
        @DynamicProperty(name = "Name of the field to add",
                supportsExpressionLanguage = true,
                value = "Value of the field to add",
                description = "Add a field to the record with the specified value. Expression language can be used." +
                        "You can not add a field that end with '.type' as this suffix is used to specify the type of fields to add",
                nameForDoc = "fakeField"),
        @DynamicProperty(name = "Name of the field to add with the suffix '"+ AddFields.DYNAMIC_PROPS_TYPE_SUFFIX +"'",
                supportsExpressionLanguage = false,
                value = "Type of the field to add",
                description = "Add a field to the record with the specified type. These properties are only used if a correspondant property without" +
                        " the suffix '"+ AddFields.DYNAMIC_PROPS_TYPE_SUFFIX +"' is already defined. If this property is not defined, default type for adding fields is String." +
                        "You can only use Logisland predefined type fields.",
                nameForDoc = "fakeField" + AddFields.DYNAMIC_PROPS_TYPE_SUFFIX),
        @DynamicProperty(name = "Name of the field to add with the suffix '" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX + "'",
                supportsExpressionLanguage = true,
                value = "Name of the field to add using expression language",
                description = "Add a field to the record with the specified name (which is evaluated using expression language). " +
                        "These properties are only used if a correspondant property without" +
                        " the suffix '" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX + "' is already defined. If this property is not defined, " +
                        "the name of the field to add is the key of the first dynamic property (which is the main and only required dynamic property).",
                nameForDoc = "fakeField" + AddFields.DYNAMIC_PROPS_NAME_SUFFIX)
        })
public class AddFields extends AbstractProcessor {


    private static final Logger logger = LoggerFactory.getLogger(AddFields.class);

    public static final String DYNAMIC_PROPS_TYPE_SUFFIX = ".field.type";
    public static final String DYNAMIC_PROPS_NAME_SUFFIX = ".field.name";

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

    private final Set<PropertyDescriptor> dynamicFieldProperties = new HashSet<>();
    private final  Map<String, PropertyDescriptor> dynamicTypeProperties = new HashMap<>();
    private final Map<String, PropertyDescriptor> dynamicNameProperties = new HashMap<>();
    private String conflictPolicy;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(false)
                    .addValidator(new StandardValidators.EnumValidator(FieldType.class))
                    .allowableValues(FieldType.values())
                    .defaultValue(FieldType.STRING.getName().toUpperCase())
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_NAME_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }


    @Override
    public void init(ProcessContext context)  throws InitializationException {
        super.init(context);
        initDynamicProperties(context);
        this.conflictPolicy = context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString();
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        for (Record record : records) {
            updateRecord(context, record);
        }
        return records;
    }

    private void updateRecord(ProcessContext context, Record record) {
        dynamicFieldProperties.forEach(addedFieldDescriptor -> {
            final PropertyValue propValueOfValue = context.getPropertyValue(addedFieldDescriptor).evaluate(record);
            String fieldNameToAdd = addedFieldDescriptor.getName();
            if (dynamicNameProperties.containsKey(fieldNameToAdd)) {
                fieldNameToAdd = context.getPropertyValue(dynamicNameProperties.get(fieldNameToAdd)).evaluate(record).asString();
            }
            if (!record.hasField(fieldNameToAdd) || conflictPolicy.equals(OVERWRITE_EXISTING.getValue())) {
                setFieldValue(record, fieldNameToAdd, propValueOfValue, context);
            }
        });
    }

    private void setFieldValue(Record record, String fieldName, PropertyValue newValue, ProcessContext context) {
        if (dynamicTypeProperties.containsKey(fieldName) && dynamicTypeProperties.get(fieldName).isDynamic()) {// un type été configuré
            FieldType fieldType = FieldType.valueOf(context.getPropertyValue(dynamicTypeProperties.get(fieldName)).asString());
            record.setField(fieldName, fieldType, newValue.getRawValue());
        } else {// pas de type configuré
            if (record.hasField(fieldName)) { //utilise l'ancien type
                updateRecord(record, fieldName, newValue);
            } else {//utilise le type String par défaut
                record.setStringField(fieldName, newValue.asString());
            }
        }
    }

    private void updateRecord(Record record, String fieldName, PropertyValue newValue) {
        final Field fieldToUpdate = record.getField(fieldName);
        record.removeField(fieldName);
        record.setField(fieldName, fieldToUpdate.getType(), newValue.getRawValue());
    }

    private void initDynamicProperties(ProcessContext context) {
        dynamicFieldProperties.clear();
        dynamicTypeProperties.clear();
        dynamicNameProperties.clear();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
                dynamicTypeProperties.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_TYPE_SUFFIX), entry.getKey());
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_NAME_SUFFIX)) {
                dynamicNameProperties.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_NAME_SUFFIX), entry.getKey());
                continue;
            }
            dynamicFieldProperties.add(entry.getKey());
        }
    }
}
