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

import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ExtraDetailFile("./details/common-processors/SplitTextWithProperties-Detail.rst")
public class SplitTextWithProperties extends SplitText {

    private static final long serialVersionUID = 4180349996855996949L;

    private static final Logger logger = LoggerFactory.getLogger(SplitTextWithProperties.class);

    public static final PropertyDescriptor PROPERTIES_FIELD = new PropertyDescriptor.Builder()
            .name("properties.field")
            .description("the field containing the properties to split and treat")
            .required(true)
            .defaultValue("properties")
            .build();

    private static final Pattern PROPERTIES_SPLITTER_PATTERN = Pattern.compile("([\\S]+)=([^=]+)(?:\\s|$)");

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(VALUE_REGEX);
        descriptors.add(VALUE_FIELDS);
        descriptors.add(KEY_REGEX);
        descriptors.add(KEY_FIELDS);
        descriptors.add(RECORD_TYPE);
        descriptors.add(KEEP_RAW_CONTENT);
        descriptors.add(PROPERTIES_FIELD);

        return Collections.unmodifiableList(descriptors);
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        List<Record> outputRecords = (List<Record>) super.process(context, records);

        String propertiesField = context.getPropertyValue(PROPERTIES_FIELD).asString();

        for (Record outputRecord : outputRecords) {
            Field field = outputRecord.getField(propertiesField);
            if (field != null) {
                String str = field.getRawValue().toString();
                Matcher matcher = PROPERTIES_SPLITTER_PATTERN.matcher(str);
                while (matcher.find()) {
                    if (matcher.groupCount() == 2) {
                        String key = matcher.group(1);
                        String value = matcher.group(2);

                        // logger.debug(String.format("field %s = %s", key, value));
                        outputRecord.setField(key, FieldType.STRING, value);
                    }
                }
                outputRecord.removeField("properties");
            }
        }
        return outputRecords;
    }

}
