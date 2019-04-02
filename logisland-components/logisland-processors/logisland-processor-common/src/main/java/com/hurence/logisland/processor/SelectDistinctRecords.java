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
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Tags({"record", "fields", "remove", "delete"})
@CapabilityDescription("Keep only distinct records based on a given field")
@ExtraDetailFile("./details/common-processors/SelectDistinctRecords-Detail.rst")
public class SelectDistinctRecords extends AbstractProcessor {

    private static final long serialVersionUID = -270933070438408174L;

    private static final Logger logger = LoggerFactory.getLogger(SelectDistinctRecords.class);

    public static final PropertyDescriptor FILTERING_FIELD = new PropertyDescriptor.Builder()
            .name("field.name")
            .description("the field to distinct records")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(FieldDictionary.RECORD_ID)
            .build();


    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String fieldToFilterOn = context.getPropertyValue(FILTERING_FIELD).asString();
        Collection<Record> outputRecords = records.stream().filter(record -> !record.hasField(fieldToFilterOn)).collect(Collectors.toList());
        try {



            outputRecords.addAll(
                    records.stream()
                    .filter(distinctByKey(record -> record.getField(fieldToFilterOn)))
                    .collect(Collectors.toList()));



        } catch (Exception ex) {
            logger.warn("issue while trying to remove field list {} :  {}",
                    context.getPropertyValue(FILTERING_FIELD).asString(),
                    ex.toString());
        }

        return outputRecords;
    }


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(
                Lists.newArrayList(FILTERING_FIELD));
    }
}
