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

import com.hurence.logisland.processor.agg.Agg;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import com.hurence.logisland.validator.ValidationContext;
import com.hurence.logisland.validator.ValidationResult;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


@Tags({"record", "fields", "agg", "aggregation", "metric", "stat"})
@CapabilityDescription("Enable to compute some common aggregations on all records or on array fields of records")
@ExtraDetailFile("./details/common-processors/ComputeAggsProcessor-Detail.rst")
public class ComputeAggsProcessor extends AbstractProcessor {

    public static final String DYNAMIC_PROPS_AGG_TYPE_SUFFIX = ".agg";
    public static final String DYNAMIC_PROPS_TYPE_SUFFIX = ".type";

    private final Set<PropertyDescriptor> dynamicFieldProperties = new HashSet<>();
    private final  Map<String, PropertyDescriptor> dynamicAggTypeProperties = new HashMap<>();
    private final  Map<String, PropertyDescriptor> dynamicTypeResultProperties = new HashMap<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> validations = new ArrayList<>();
        Set<Map.Entry<PropertyDescriptor, String>> props = context.getProperties().entrySet();
        if (props.isEmpty()) {
            validations.add(new ValidationResult.Builder()
                    .input("processor conf")
                    .explanation("processor must have at least one aggregation configured otherwise just delete this processor")
                    .valid(false)
                    .build()
            );
        }
        final Set<PropertyDescriptor> dynamicFieldPropertiesTmp = new HashSet<>();
        final Map<String, PropertyDescriptor> dynamicAggTypePropertiesTmp = new HashMap<>();
        final Map<String, PropertyDescriptor> dynamicTypeResultPropertiesTmp = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : props) {//TODO
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_AGG_TYPE_SUFFIX)) {
                dynamicAggTypePropertiesTmp.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_AGG_TYPE_SUFFIX), entry.getKey());
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
                dynamicTypeResultPropertiesTmp.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_TYPE_SUFFIX), entry.getKey());
                continue;
            }
            dynamicFieldPropertiesTmp.add(entry.getKey());
        }
        dynamicFieldPropertiesTmp.forEach(desc -> {
            if (!dynamicAggTypePropertiesTmp.containsKey(desc.getName())) {
                validations.add(new ValidationResult.Builder()
                        .input(desc.getName())
                        .explanation(String.format("You must configure property '%s' to specify the kind of agg to use",
                                desc.getName() + DYNAMIC_PROPS_AGG_TYPE_SUFFIX))
                        .valid(false)
                        .build()
                );
            }
            if (!dynamicTypeResultPropertiesTmp.containsKey(desc.getName())) {
                validations.add(new ValidationResult.Builder()
                        .input(desc.getName())
                        .explanation(String.format("You must configure property '%s' to specify the agg result type",
                                desc.getName() + DYNAMIC_PROPS_TYPE_SUFFIX))
                        .valid(false)
                        .build()
                );
            }
        });
        //test that there is a field to create with associated field for each type result or kind of aggs
        dynamicAggTypePropertiesTmp.keySet().forEach(key -> {
            Optional<PropertyDescriptor> foundDesc = dynamicFieldPropertiesTmp.stream()
                    .filter(desc -> desc.getName().equals(key))
                    .findAny();
            if (!foundDesc.isPresent()) {
                validations.add(new ValidationResult.Builder()
                        .input(key)
                        .explanation(String.format(
                                "You must configure property '%s' to specify the field to create and on which field to compute aggregation.",
                                key))
                        .valid(false)
                        .build()
                );
            }
        });
        dynamicTypeResultPropertiesTmp.keySet().forEach(key -> {
            Optional<PropertyDescriptor> foundDesc = dynamicFieldPropertiesTmp.stream()
                    .filter(desc -> desc.getName().equals(key))
                    .findAny();
            if (!foundDesc.isPresent()) {
                validations.add(new ValidationResult.Builder()
                        .input(key)
                        .explanation(String.format(
                                "You must configure property '%s' to specify the field to create and on which field to compute aggregation.",
                                key))
                        .valid(false)
                        .build()
                );
            }
        });
        return  validations;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_AGG_TYPE_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(false)
                    .addValidator(new StandardValidators.EnumValidator(Agg.class))
                    .allowableValues(Agg.values())
                    .required(false)
                    .dynamic(true)
                    .build();
        }
        if (propertyDescriptorName.endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .expressionLanguageSupported(false)
                    .addValidator(new StandardValidators.EnumValidator(FieldType.class))
                    .allowableValues(FieldType.DOUBLE.toString().toUpperCase(), FieldType.LONG.toString().toUpperCase(),
                            FieldType.INT.toString().toUpperCase(), FieldType.FLOAT.toString().toUpperCase())
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
    public void init(final ProcessContext context) {
        super.init(context);
        initDynamicProperties(context);
    }

    private void initDynamicProperties(final ProcessContext context) {
        dynamicFieldProperties.clear();
        dynamicAggTypeProperties.clear();
        dynamicTypeResultProperties.clear();
        // loop over dynamic properties to add alternative regex
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (!entry.getKey().isDynamic()) {
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_AGG_TYPE_SUFFIX)) {
                dynamicAggTypeProperties.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_AGG_TYPE_SUFFIX), entry.getKey());
                continue;
            }
            if (entry.getKey().getName().endsWith(DYNAMIC_PROPS_TYPE_SUFFIX)) {
                dynamicTypeResultProperties.put(StringUtils.removeEnd(entry.getKey().getName(), DYNAMIC_PROPS_TYPE_SUFFIX), entry.getKey());
                continue;
            }
            dynamicFieldProperties.add(entry.getKey());
        }
    }

    @Override
    public Collection<Record> process(final ProcessContext context, Collection<Record> records) {
        return records.stream()
                .map(r -> computeAggs(context, r))
                .collect(Collectors.toList());
    }

    private Record computeAggs(ProcessContext context, Record record) {
        dynamicFieldProperties.forEach(mergeFieldDescriptor -> {
            final String fieldNameToComputeAgg = context.getPropertyValue(mergeFieldDescriptor).asString();
            String fieldNameToAdd = mergeFieldDescriptor.getName();
            if (!record.hasField(fieldNameToComputeAgg)) return;
            Field fieldToAggregate =  record.getField(fieldNameToComputeAgg);
            if (!FieldType.ARRAY.equals(fieldToAggregate.getType())) return;
            Agg aggType = Agg.valueOf(context.getPropertyValue(dynamicAggTypeProperties.get(fieldNameToAdd)).asString());
            FieldType type = FieldType.valueOf(context.getPropertyValue(dynamicTypeResultProperties.get(fieldNameToAdd)).asString());
            Number aggValue = null;
            try {
                List<? extends Number> values = (List<? extends Number>) fieldToAggregate.getRawValue();
                switch (type) {
                    case INT:
                        aggValue = computeAgg(aggType, values.stream().mapToInt(Number::intValue));
                        break;
                    case LONG:
                        aggValue = computeAgg(aggType, values.stream().mapToLong(Number::longValue));
                        break;
                    case FLOAT:
                        aggValue = computeAgg(aggType, values.stream().mapToDouble(Number::floatValue)).floatValue();
                        break;
                    case DOUBLE:
                        aggValue = computeAgg(aggType, values.stream().mapToDouble(Number::doubleValue));
                        break;
                    case ARRAY:
                    case NULL:
                    case STRING:
                    case BYTES:
                    case RECORD:
                    case MAP:
                    case ENUM:
                    case BOOLEAN:
                    case UNION:
                    case DATETIME:
                        return;//do nothing
                }
                record.setField(fieldNameToAdd, type, aggValue);
            } catch (Exception ex) {
                record.addError("ERROR TYPE", getLogger(),
                        "error while computing aggregate '{}' on field '{}'",
                        new Object[]{aggType, fieldNameToComputeAgg});
            }
        });
        return record;
    }

    private Number computeAgg(Agg aggType, DoubleStream stream) {
        switch (aggType) {
            case MAX:
                return stream.max().orElseThrow(NoSuchElementException::new);
            case MIN:
                return stream.min().orElseThrow(NoSuchElementException::new);
            case AVG:
                return stream.average().orElseThrow(NoSuchElementException::new);
            default:
                throw new IllegalArgumentException("Invalid agg type '" + aggType + "'");
        }
    }


    private Number computeAgg(Agg aggType, IntStream stream) {
        switch (aggType) {
            case MAX:
                return stream.max().orElseThrow(NoSuchElementException::new);
            case MIN:
                return stream.min().orElseThrow(NoSuchElementException::new);
            case AVG:
                return stream.average().orElseThrow(NoSuchElementException::new);
            default:
                throw new IllegalArgumentException("Invalid agg type '" + aggType + "'");
        }
    }


    private Number computeAgg(Agg aggType, LongStream stream) {
        switch (aggType) {
            case MAX:
                return stream.max().orElseThrow(NoSuchElementException::new);
            case MIN:
                return stream.min().orElseThrow(NoSuchElementException::new);
            case AVG:
                return stream.average().orElseThrow(NoSuchElementException::new);
            default:
                throw new IllegalArgumentException("Invalid agg type '" + aggType + "'");
        }
    }
}

