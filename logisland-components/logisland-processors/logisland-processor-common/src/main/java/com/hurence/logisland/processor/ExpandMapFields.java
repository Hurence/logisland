/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.processor;

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

import java.util.*;

@Tags({"record", "fields", "Expand", "Map"})
@CapabilityDescription("Expands the content of a MAP field to the root.")
public class ExpandMapFields extends AbstractProcessor {

    public static final PropertyDescriptor FIELDS_TO_EXPAND = new PropertyDescriptor.Builder()
            .name("fields.to.expand")
            .required(true)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .displayName("Fields to expand")
            .description("Comma separated list of fields of type map that will be expanded to the root")
            .build();

    private static final Logger logger = LoggerFactory.getLogger(AddFields.class);


    public static final AllowableValue OVERWRITE_EXISTING =
            new AllowableValue("overwrite_existing", "overwrite existing field", "if field already exist");

    public static final AllowableValue KEEP_OLD_FIELD =
            new AllowableValue("keep_only_old_field", "keep only old field value", "keep only old field");


    public static final PropertyDescriptor CONFLICT_RESOLUTION_POLICY = new PropertyDescriptor.Builder()
            .name("conflict.resolution.policy")
            .description("What to do when a field with the same name already exists ?")
            .required(false)
            .defaultValue(KEEP_OLD_FIELD.getValue())
            .allowableValues(OVERWRITE_EXISTING, KEEP_OLD_FIELD)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(FIELDS_TO_EXPAND, CONFLICT_RESOLUTION_POLICY);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        final List<String> fieldNames = Arrays.asList(context.getPropertyValue(FIELDS_TO_EXPAND).asString().split(","));
        boolean overwrite = OVERWRITE_EXISTING.getValue().equals(context.getPropertyValue(CONFLICT_RESOLUTION_POLICY).asString());
        records.forEach(record ->
                fieldNames.forEach(fieldName -> {
                    if (record.hasField(fieldName)) {
                        Field field = record.removeField(fieldName);
                        if (field.getType() == FieldType.MAP && field.getRawValue() instanceof Map) {
                            ((Map<String, ?>) field.getRawValue()).forEach((k, v) -> {
                                if (overwrite || !record.hasField(k)) {
                                    record.setField(doDeserializeField(k, v));
                                }
                            });
                        }
                    }
                }));
        return records;
    }

    private Field doDeserializeField(String name, Object value) {
        Field ret;
        if (value == null) {
            ret = new Field(name, FieldType.NULL, null);
        } else if (value instanceof List) {
            ret = new Field(name, FieldType.ARRAY, value);
        } else if (value instanceof Map) {
            ret = new Field(name, FieldType.MAP, value);
        } else {
            if (value instanceof Boolean) {
                ret = new Field(name, FieldType.BOOLEAN, value);
            } else if (value instanceof Float) {
                ret = new Field(name, FieldType.FLOAT, value);
            } else if (value instanceof Long) {
                ret = new Field(name, FieldType.LONG, value);
            } else if (value instanceof Number) {
                ret = new Field(name, FieldType.INT, value);
            } else if (value instanceof Double) {
                ret = new Field(name, FieldType.DOUBLE, value);
            } else if (value instanceof Date) {
                ret = new Field(name, FieldType.DATETIME, value);
            } else {
                ret = new Field(name, FieldType.STRING, value);
            }
        }
        return ret;
    }
}
