/*
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
 *
 */

package com.hurence.logisland.engine.vanilla.stream.kafka;

import com.hurence.logisland.component.ComponentContext;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.logging.ComponentLog;
import com.hurence.logisland.stream.AbstractRecordStream;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaParallelProcessingStream extends AbstractRecordStream {

    private final ComponentLog logger = getLogger();

    private KafkaStreams streams;

    private static final PropertyDescriptor PROPERTY_APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("application.id")
            .description("The application ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    private static final PropertyDescriptor PROPERTY_BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .description("List of kafka nodes to connect to")
            .addValidator(StandardValidators.COMMA_SEPARATED_LIST_VALIDATOR)
            .required(true)
            .build();

    @Override

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(PROPERTY_APPLICATION_ID, PROPERTY_BOOTSTRAP_SERVERS);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        ConfigDef.ConfigKey ck = StreamsConfig.configDef().configKeys().get(propertyDescriptorName);
        if (ck != null) {
            return new PropertyDescriptor.Builder()
                    .dynamic(true)
                    .name(propertyDescriptorName)
                    .description(ck.documentation)
                    .required(ck.importance.equals(ConfigDef.Importance.HIGH))
                    .build();
        }
        logger.warn("Unknown property " + propertyDescriptorName);
        return null;
    }

    @Override
    public void init(ComponentContext context) {
        super.init(context);
        Topology topology = new Topology();
        Properties properties = new Properties();
        context.getProperties().forEach((propertyDescriptor, s) -> properties.put(propertyDescriptor.getName(), s));
        streams = new KafkaStreams(topology, properties);

    }

    @Override
    public void start() {
        super.start();
        streams.start();

    }

    @Override
    public void stop() {
        try {
            streams.close();
        } finally {
            super.stop();
        }
    }
}
