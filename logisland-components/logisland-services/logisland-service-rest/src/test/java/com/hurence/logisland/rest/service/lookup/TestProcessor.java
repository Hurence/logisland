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

package com.hurence.logisland.rest.service.lookup;

import com.hurence.logisland.classloading.PluginProxy;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.component.PropertyValue;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.service.lookup.LookupService;
import com.hurence.logisland.service.lookup.RecordLookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class TestProcessor extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(TestProcessor.class);

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("lookup.service")
            .description("LookupService test processor")
            .identifiesControllerService(LookupService.class)
            .required(true)
            .build();

    // Ip to Geo service to use to perform the translation requests
    private RecordLookupService lookupService = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(LOOKUP_SERVICE);
        return propDescs;
    }

    @Override
    public boolean hasControllerService() {
        return true;
    }

    @Override
    public void init(final ProcessContext context) {
        lookupService = PluginProxy.rewrap(context.getPropertyValue(LOOKUP_SERVICE).asControllerService());
        if (lookupService == null) {
            logger.error("LookupService service is not initialized!");
        }
    }

    @Override
    public Collection<Record> process(ProcessContext context, Collection<Record> records) {
         List<Record> toReturn = records.stream()
                .map(this::getRestResponseFromRecord)
                 .flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
                .collect(Collectors.toList());
         return toReturn;
    }

    private Optional<Record> getRestResponseFromRecord(Record record) {
        try {
            return lookupService.lookup(record);
        } catch (LookupFailureException e) {
            String error = e.getMessage();
            logger.error(error, e);
            record.addError("lookup_error", error);
            return Optional.of(record);
        }
    }


}
