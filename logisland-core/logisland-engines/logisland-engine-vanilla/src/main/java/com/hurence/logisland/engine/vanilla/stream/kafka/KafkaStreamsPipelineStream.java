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
import com.hurence.logisland.stream.StreamContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaStreamsPipelineStream extends AbstractRecordStream {

    private final ComponentLog logger = getLogger();

    private KafkaStreams streams;


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(
                StreamProperties.PROPERTY_BOOTSTRAP_SERVERS,
                StreamProperties.READ_TOPICS,
                StreamProperties.AVRO_INPUT_SCHEMA,
                StreamProperties.AVRO_OUTPUT_SCHEMA,
                StreamProperties.KAFKA_MANUAL_OFFSET_RESET,
                StreamProperties.READ_TOPICS_SERIALIZER,
                StreamProperties.WRITE_TOPICS,
                StreamProperties.WRITE_TOPICS_SERIALIZER
        );
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
        StreamContext streamContext = (StreamContext) context;
        Topology topology = new Topology();
        Properties properties = new Properties();
        String streamId = streamContext.getStream().getIdentifier();
        String sourceId = "source_" + streamId;
        String pipelineId = "pipeline_" + streamId;
        String sinkId = "sink_" + streamId;
        properties.put(StreamProperties.PROPERTY_APPLICATION_ID.getName(), streamId);
        properties.put(StreamProperties.PROPERTY_BOOTSTRAP_SERVERS.getName(),
                streamContext.getPropertyValue(StreamProperties.PROPERTY_BOOTSTRAP_SERVERS).asString());

        context.getProperties().forEach((propertyDescriptor, s) -> {
            if (propertyDescriptor.isDynamic()) {
                properties.put(propertyDescriptor.getName(), s);
            }
        });
        topology
                .addSource(Topology.AutoOffsetReset.valueOf(StringUtils.upperCase(streamContext.getPropertyValue(StreamProperties.KAFKA_MANUAL_OFFSET_RESET).asString())),
                        sourceId, streamContext.getPropertyValue(StreamProperties.READ_TOPICS).asString().split(","))
                .addProcessor(pipelineId,
                        () -> new LogislandPipelineProcessor(streamContext),
                        sourceId);
        for (String outTopic : streamContext.getPropertyValue(StreamProperties.WRITE_TOPICS).asString().split(",")) {
            topology.addSink(sinkId + "_" + outTopic, outTopic, new ByteArraySerializer(),
                    new ByteArraySerializer(), pipelineId);
        }
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
