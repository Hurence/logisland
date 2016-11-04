/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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

import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SplitTextMultiline extends AbstractProcessor {

    static final long serialVersionUID = -1L;

    private static Logger logger = LoggerFactory.getLogger(SplitTextMultiline.class);


    public static final PropertyDescriptor REGEX = new PropertyDescriptor.Builder()
            .name("regex")
            .description("the regex to match")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .description("a comma separated list of fields corresponding to matching groups")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("event.type")
            .description("the type of event")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private String buffer = "";


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REGEX);
        descriptors.add(FIELDS);
        descriptors.add(EVENT_TYPE);

        return Collections.unmodifiableList(descriptors);
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        final String[] fields = context.getPropertyValue(FIELDS).asString().split(",");
        final String regexString = context.getPropertyValue(REGEX).asString();
        final String eventType = context.getPropertyValue(EVENT_TYPE).asString();
        final Pattern regex = Pattern.compile(regexString, Pattern.DOTALL);

        List<Record> outputRecords = new ArrayList<>();


        records.forEach(record -> {
            try {
                final String key = record.getField(FieldDictionary.RECORD_KEY).asString();
                buffer += record.getField(FieldDictionary.RECORD_VALUE).asString();

            } catch (Exception ex) {
                logger.info("probleme getting K/V on record {}exception {}", record, ex.getMessage());
            }
        });


        Matcher matcher = regex.matcher(buffer);
        if (matcher.find()) {


            Record record = new StandardRecord(eventType);
            for (int i = 0; i < matcher.groupCount() + 1 && i < fields.length; i++) {
                String content = matcher.group(i);
                if (content != null) {
                    record.setStringField(fields[i], matcher.group(i).replaceAll("\"", ""));
                }

            }
            outputRecords.add(record);
            buffer = "";
        } else {
            logger.warn("no match");
        }


        return outputRecords;
    }

}
