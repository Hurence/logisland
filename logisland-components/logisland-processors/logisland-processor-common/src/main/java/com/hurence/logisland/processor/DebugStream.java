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

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.serializer.JsonSerializer;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.StringSerializer;
import com.hurence.logisland.validator.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Tags({"record", "debug"})
@CapabilityDescription("This is a processor that logs incoming records")
public class DebugStream extends AbstractProcessor {

    private static Logger logger = LoggerFactory.getLogger(DebugStream.class);


    public static final AllowableValue JSON = new AllowableValue("json", "Json serialization",
            "serialize events as json blocs");

    public static final AllowableValue STRING = new AllowableValue("string", "String serialization",
            "serialize events as toString() blocs");

    public static final PropertyDescriptor SERIALIZER = new PropertyDescriptor.Builder()
            .name("event.serializer")
            .description("the way to serialize event")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(JSON.getValue())
            .allowableValues(JSON, STRING)
            .build();

    public static final PropertyDescriptor JVM_DUMP = new PropertyDescriptor.Builder()
            .name("jvm.dump")
            .description("if processor should log jvm dump or not")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();


    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERIALIZER);
        descriptors.add(JVM_DUMP);
        return Collections.unmodifiableList(descriptors);
    }

    private volatile MemoryMXBean memBean;


    @Override
    public void init(ProcessContext context) {
        if (memBean == null) {
            memBean = ManagementFactory.getMemoryMXBean();
        }
    }

    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> collection) {

        if (context.getPropertyValue(JVM_DUMP).asBoolean()) {
            System.gc();
            getLogger().info("heap mem after gc: {} in processor {}", new Object[]{memBean.getHeapMemoryUsage(), this.getIdentifier()});
            getLogger().info("non heap mem after gc: {} in processor {}", new Object[]{memBean.getNonHeapMemoryUsage(), this.getIdentifier()});
        }

        if (collection.size() != 0) {
            RecordSerializer serializer = null;
            if (context.getPropertyValue(SERIALIZER).asString().equals(JSON.getValue())) {
                serializer = new JsonSerializer();
            } else {
                serializer = new StringSerializer();
            }


            final RecordSerializer finalSerializer = serializer;
            collection.forEach(event -> {


                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                finalSerializer.serialize(baos, event);
                try {
                    baos.close();
                } catch (IOException e) {
                    logger.debug("error {} ", e.getCause());
                }

                logger.info(new String(baos.toByteArray()));


            });
        }


        return collection;
    }


}
