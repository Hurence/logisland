package com.hurence.logisland.processor;

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


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.converter.RecordsTimeSeriesConverter;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

@Tags({"record", "fields", "timeseries", "chronix", "convert"})
@CapabilityDescription("Converts a given field records into a chronix timeseries record")
@ExtraDetailFile("./details/common-processors/EncodeSAX-Detail.rst")
public class ConvertToTimeseries extends AbstractProcessor {


    private Logger logger = LoggerFactory.getLogger(ConvertToTimeseries.class.getName());
    private RecordsTimeSeriesConverter converter = new RecordsTimeSeriesConverter();


    public static final PropertyDescriptor GROUP_BY_FIELD = new PropertyDescriptor.Builder()
            .name("group.by.field")
            .description("The field the chunk should be grouped by")
            .required(false)
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .defaultValue("")
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(GROUP_BY_FIELD);

        return descriptors;
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {

        String[] groupBy = context.getPropertyValue(GROUP_BY_FIELD).asString().split(",");

        List<Record> outputRecords = Collections.emptyList();

        Map<String, List<Record>> groups = records.stream().collect(Collectors.groupingBy(r ->
                Arrays.stream(groupBy).filter(StringUtils::isNotBlank).collect(Collectors.toList())
                        .stream().map(f -> r.hasField(f) ? r.getField(f).asString() : null)
                        .collect(Collectors.joining("|"))));

        if (!groups.isEmpty()) {
            outputRecords = groups.values().stream()
                    .filter(l -> !l.isEmpty())
                    .map(recs -> {
                        Collections.sort(recs, Comparator.comparing(Record::getTime));
                        return recs;
                    })
                    .map(converter::chunk)
                    .collect(Collectors.toList());
        }

        return outputRecords;
    }

}

