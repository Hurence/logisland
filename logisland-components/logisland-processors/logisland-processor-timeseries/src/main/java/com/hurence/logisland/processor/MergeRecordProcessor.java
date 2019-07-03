package com.hurence.logisland.processor;

/**
 * Copyright (C) 2019 Hurence (support@hurence.com)
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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;
import java.util.stream.Collectors;


@Tags({"record", "fields", "timeseries", "chronix", "convert"})
@CapabilityDescription("Converts a given field records into a chronix timeseries record")
@ExtraDetailFile("./details/common-processors/EncodeSAX-Detail.rst")
public class MergeRecordProcessor extends AbstractProcessor {

    public static final String DYNAMIC_PROPS_TYPE_SUFFIX = ".field.type";
    public static final String DYNAMIC_PROPS_NAME_SUFFIX = ".field.name";

    private static final AllowableValue TAKE_FIRST_STRATEGY =
            new AllowableValue("take_first", "Take value of first record", "will take value of first record");

    private static final AllowableValue ARRAY_STRATEGY =
            new AllowableValue("array", "Aggregate values into one array", "will aggregate values into one array");

    public static final PropertyDescriptor GROUP_BY_FIELD = new PropertyDescriptor.Builder()
            .name("group.by.field")
            .description("The field to use to group records to aggregate into one. There will be one record in output by different value of the field")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)//TODO list or not ?
            .build();

    public static final PropertyDescriptor DEFAULT_STRATEGY = new PropertyDescriptor.Builder()
            .name("group.by.field")
            .description("The field to use to group records to aggregate into one. There will be one record in output by different value of the field")
            .required(false)
            .allowableValues(TAKE_FIRST_STRATEGY, ARRAY_STRATEGY)
            .defaultValue(TAKE_FIRST_STRATEGY.getValue())
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUP_BY_FIELD);
        descriptors.add(DEFAULT_STRATEGY);
        return descriptors;
    }

    //    values: value
//    values.strategy: array
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        //TODO
//        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
//            return new PropertyDescriptor.Builder()
//                    .name(propertyDescriptorName)
//                    .expressionLanguageSupported(false)
//                    .addValidator(new StandardValidators.EnumValidator(FieldType.class))
//                    .allowableValues(FieldType.values())
//                    .defaultValue(FieldType.STRING.getName().toUpperCase())
//                    .required(false)
//                    .dynamic(true)
//                    .build();
//        }
//        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_NAME_SUFFIX)) {
//            return new PropertyDescriptor.Builder()
//                    .name(propertyDescriptorName)
//                    .expressionLanguageSupported(true)
//                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//                    .required(false)
//                    .dynamic(true)
//                    .build();
//        }
//        return new PropertyDescriptor.Builder()
//                .name(propertyDescriptorName)
//                .expressionLanguageSupported(true)
//                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//                .required(false)
//                .dynamic(true)
//                .build();
        return null;
    }

    private List<String> groupBy;

    @Override
    public void init(final ProcessContext context) {
        super.init(context);
       //TODO
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        List<Record> outputRecords = Collections.emptyList();

        Map<String, List<Record>> groups = records.stream().collect(
                Collectors.groupingBy(r ->
                        groupBy
                                .stream().map(f -> r.hasField(f) ? r.getField(f).asString() : null)
                                .collect(Collectors.joining("|"))
                ));

        if (!groups.isEmpty()) {
            //TODO
//            outputRecords = groups.values().stream()
//                    .filter(l -> !l.isEmpty())
//                    .peek(recs -> {
//                        recs.sort(Comparator.comparing(Record::getTime));
//                    })
//                    .map(converter::chunk)
//                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

}

