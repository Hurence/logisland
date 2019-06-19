package com.hurence.logisland.processor;

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


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.*;
import com.hurence.logisland.timeseries.converter.RecordsTimeSeriesConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


@Tags({"record", "fields", "timeseries", "chronix", "convert"})
@CapabilityDescription("Converts chronix timeseries records into standard records")
@ExtraDetailFile("./details/common-processors/EncodeSAX-Detail.rst")
public class ConvertFromTimeseries extends AbstractProcessor {


    private Logger logger = LoggerFactory.getLogger(ConvertFromTimeseries.class.getName());
    private RecordsTimeSeriesConverter converter = new RecordsTimeSeriesConverter();



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
       // descriptors.add(GROUP_BY_FIELD);

        return descriptors;
    }


    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
        return records.stream()
                .flatMap(r -> converter.unchunk(r).stream())
                .collect(Collectors.toList());
    }

}


