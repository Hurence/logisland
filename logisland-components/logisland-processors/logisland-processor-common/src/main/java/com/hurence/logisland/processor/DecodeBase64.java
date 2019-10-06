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
package com.hurence.logisland.processor;

import com.google.common.collect.ImmutableList;
import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.util.string.BinaryEncodingUtils;
import com.hurence.logisland.validator.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Tags({"decode", "base64"})
@CapabilityDescription("Decodes fields to base64. The fields should be of type string")
@ExtraDetailFile("./details/common-processors/DecodeBase64-Detail.rst")
public class DecodeBase64 extends AbstractProcessor {

    static final PropertyDescriptor SOURCE_FIELDS = new PropertyDescriptor.Builder()
            .name("source.fields")
            .description("a comma separated list of fields corresponding to the fields to decode. Please note than the " +
                    "fields should be of type string")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor DESTINATION_FIELDS = new PropertyDescriptor.Builder()
            .name("destination.fields")
            .description("a comma separated list of fields corresponding to the decoded content according to the " +
                    "fields provided as input. Those fields will be of type bytes")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .build();

    private final List<String> sourceFieldNames = new ArrayList<>();
    private final List<String> destinationFieldNames = new ArrayList<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(SOURCE_FIELDS, DESTINATION_FIELDS);
    }

    @Override
    public void init(ProcessContext context) throws InitializationException {
        super.init(context);
        context.getPropertyValue(SOURCE_FIELDS).asStringOpt().ifPresent(s ->
                sourceFieldNames.addAll(Arrays.asList((s.split(",")))));
        context.getPropertyValue(DESTINATION_FIELDS).asStringOpt().ifPresent(s ->
                destinationFieldNames.addAll(Arrays.asList((s.split(",")))));
        if (sourceFieldNames.size() != destinationFieldNames.size()) {
            throw new InitializationException(String.format("Processor properties %s and %s must contains the same number of elements. " +
                            "Actual are: %d and %d", SOURCE_FIELDS.getName(), DESTINATION_FIELDS.getName(),
                    sourceFieldNames.size(), destinationFieldNames.size()));

        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        records.forEach(record -> {
            for (int i = 0; i < sourceFieldNames.size(); i++) {
                String sourceField = sourceFieldNames.get(i);
                Field f = record.getField(sourceField);
                if (f != null) {
                    if (!(f.getType() == FieldType.STRING || f.getType() == FieldType.NULL)) {
                        record.addError("FIELD TYPE", getLogger(),
                                "Field type '{}' is not a string",
                                new Object[]{f.getName()});
                    } else {
                        String content = f.asString();
                        if (content != null) {
                            try {
                                record.setBytesField(destinationFieldNames.get(i), BinaryEncodingUtils.decode(content));
                            } catch (Exception e) {
                                record.addError("PROCESSING ERROR", getLogger(),
                                        "Unable to decode field '{}' : {}",
                                        new Object[]{f.getName(), e.getMessage()});
                            }
                        }
                    }
                }
            }

        });
        return records;
    }
}
