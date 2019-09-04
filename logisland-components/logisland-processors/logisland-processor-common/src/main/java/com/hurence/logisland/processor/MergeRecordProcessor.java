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
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;


@Tags({"record", "fields", "merge", "content", "reduce"})
@CapabilityDescription("Merge contents of several records into less records.")
@ExtraDetailFile("./details/common-processors/MergeRecordProcessor-Detail.rst")
public class MergeRecordProcessor extends AbstractProcessor {

    public static final String DYNAMIC_PROPS_STRATEGY_SUFFIX = ".strategy";

    private static final AllowableValue TAKE_FIRST_STRATEGY =
            new AllowableValue("take_first", "Take value of first record", "will take value of first record");

    private static final AllowableValue ARRAY_STRATEGY =
            new AllowableValue("array", "Aggregate values into one array", "will aggregate values into one array");

    public static final PropertyDescriptor GROUP_BY_FIELD = new PropertyDescriptor.Builder()
            .name("group.by.field")
            .description("The fields to use to group records to aggregate into one. There will be one record in output by different value of the field")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor SORT_BY_FIELD = new PropertyDescriptor.Builder()
            .name("sort.by.field")
            .description("The fields to use to sort records before before aggregating into one. For example if you want to order time series value points.")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor REVERSE_SORTING = new PropertyDescriptor.Builder()
            .name("sort.reverse")
            .description("If records should be reverse sorted (if they are sorted)")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor DEFAULT_STRATEGY = new PropertyDescriptor.Builder()
            .name("default.strategy")
            .description("The default strategy to use for merging records")
            .required(false)
            .allowableValues(TAKE_FIRST_STRATEGY, ARRAY_STRATEGY)
            .defaultValue(TAKE_FIRST_STRATEGY.getValue())
            .build();

    private List<String> groupBy;
    private Comparator<Record> comparator;
    private String defaultMergeStrategy;
    private final Set<PropertyDescriptor> dynamicFieldProperties = new HashSet<>();
    private final  Map<String, PropertyDescriptor> dynamicStrategyProperties = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUP_BY_FIELD);
        descriptors.add(SORT_BY_FIELD);
        descriptors.add(REVERSE_SORTING);
        descriptors.add(DEFAULT_STRATEGY);
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_STRATEGY_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(false)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void init(final ProcessContext context) throws InitializationException {
        super.init(context);
        final String[] groupByArray = context.getPropertyValue(GROUP_BY_FIELD).asString().split(",");
        groupBy = Arrays.stream(groupByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        comparator = buildComparator(context);
        defaultMergeStrategy = context.getPropertyValue(GROUP_BY_FIELD).asString();
        initDynamicProperties(context);
    }

    private void initDynamicProperties(final ProcessContext context) {
        dynamicFieldProperties.clear();
        dynamicStrategyProperties.clear();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_STRATEGY_SUFFIX)) {
                dynamicStrategyProperties.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_STRATEGY_SUFFIX), entry.getKey());
                continue;
            }
            dynamicFieldProperties.add(entry.getKey());
        }
    }

    @Override
    public Collection<Record> process(final ProcessContext context, Collection<Record> records) {

        List<Record> outputRecords = Collections.emptyList();

        Map<String, List<Record>> groups = records.stream().collect(
                Collectors.groupingBy(r ->
                        groupBy
                                .stream().map(f -> r.hasField(f) ? r.getField(f).asString() : null)
                                .collect(Collectors.joining("|"))
                ));

        if (!groups.isEmpty()) {
            outputRecords = groups.values().stream()
                    .filter(l -> !l.isEmpty())
                    .peek(recs -> {
                        if (comparator != null) {
                            recs.sort(comparator);
                        }
                    })
                    .map(recs -> merge(context, recs))
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

    private Record merge(ProcessContext context, List<Record> records) {
        if (records.isEmpty()) return null;//should not be called with empty list
        final Record mergedRecord = new StandardRecord(records.get(0));//initialize with first record
        dynamicFieldProperties.forEach(mergeFieldDescriptor -> {
            final String fieldNameToMerge = context.getPropertyValue(mergeFieldDescriptor).asString();
            String fieldNameToAdd = mergeFieldDescriptor.getName();
            String mergeStrategy = defaultMergeStrategy;
            if (dynamicStrategyProperties.containsKey(fieldNameToAdd)) {
                mergeStrategy = context.getPropertyValue(dynamicStrategyProperties.get(fieldNameToAdd)).asString();
            }
            if (ARRAY_STRATEGY.getValue().equals(mergeStrategy)) {
                List mergeResult = records.stream()
                        .map(r -> {
                            if (r.hasField(fieldNameToMerge)) {
                                Field f = r.getField(fieldNameToMerge);
                                return f.getRawValue();
                            } else {
                                return null;
                            }
                        })
                        .collect(Collectors.toList());

                mergedRecord.setField(fieldNameToAdd, FieldType.ARRAY, mergeResult);
            } else {
                records.stream()
                    .filter(r -> r.hasField(fieldNameToMerge))
                    .map(r -> r.getField(fieldNameToMerge))
                    .findFirst()
                    .ifPresent(f ->{
                        mergedRecord.setField(fieldNameToAdd, f.getType(), f.getRawValue());
                    });
            }
        });
        return mergedRecord;
    }


    private Comparator<Record> buildComparator(ProcessContext context) {
        final String[] sortByArray = context.getPropertyValue(SORT_BY_FIELD).asString().split(",");
        final List<String> sortBy = Arrays.stream(sortByArray)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        Comparator<Record> comparator = null;
        if (!sortBy.isEmpty()) {
            comparator = Comparator.comparing(r -> r.getField(sortBy.get(0)));
            for (int i = 1; i < sortBy.size(); i++) {
                final String fieldName = sortBy.get(i);
                comparator = comparator.thenComparing(r -> r.getField(fieldName));
            }
            if (context.getPropertyValue(REVERSE_SORTING).asBoolean()) {
                comparator = comparator.reversed();
            }
        }
        return comparator;
    }
}

