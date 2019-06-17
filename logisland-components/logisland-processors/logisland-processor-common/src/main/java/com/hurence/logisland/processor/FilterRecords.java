/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.processor;

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Category(ComponentCategory.PROCESSING)
@Tags({"record", "fields", "remove", "delete"})
@CapabilityDescription("Keep only records based on a given field value")
@ExtraDetailFile("./details/common-processors/FilterRecords-Detail.rst")
public class FilterRecords extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(FilterRecords.class);

    public static final PropertyDescriptor FIELD_NAME = new PropertyDescriptor.Builder()
            .name("field.name")
            .description("the field name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_ID)
            .build();

    public static final PropertyDescriptor FIELD_VALUE = new PropertyDescriptor.Builder()
            .name("field.value")
            .description("the field value to keep")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String fieldName = context.getPropertyValue(FIELD_NAME).asString();
        String fieldValue = context.getPropertyValue(FIELD_VALUE).asString();

        return records.stream()
                .filter(record -> record.hasField(fieldName) && record.getField(fieldName).asString().equals(fieldValue))
                .collect(Collectors.toList());

    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELD_NAME);
        descriptors.add(FIELD_VALUE);

        return Collections.unmodifiableList(descriptors);
    }
}
