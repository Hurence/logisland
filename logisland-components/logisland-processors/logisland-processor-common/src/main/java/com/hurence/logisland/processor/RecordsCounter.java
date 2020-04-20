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

import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.JsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.StringSerializer;
import com.hurence.logisland.validator.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Category(ComponentCategory.UTILS)
@Tags({"record", "debug"})
@CapabilityDescription("This is a processor that logs incoming records")
@ExtraDetailFile("./details/common-processors/DebugStream-Detail.rst")
public class RecordsCounter extends AbstractProcessor {


    /**
     * Current number of elements
     */
    private static final Map<String, AtomicLong> count = new HashMap<>();


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.emptyList();
    }

    private volatile MemoryMXBean memBean;


    @Override
    public void init(ProcessContext context)  throws InitializationException {
        super.init(context);
    }

    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {


        if (collection.size() != 0) {

            collection.forEach(event -> {
                if(!count.containsKey(event.getType()))
                    count.put(event.getType(), new AtomicLong(0));
                count.get(event.getType()).incrementAndGet();
            });

            count.keySet().forEach(key ->{
                getLogger().info("RecordsCounter : " + key + " : " + count.get(key).get());
            });
        }


        return collection;
    }


}
