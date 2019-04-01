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
package com.hurence.logisland.engine.vanilla.stream.kafka;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceLookup;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.serializer.RecordSerializer;
import com.hurence.logisland.serializer.SerializerProvider;
import com.hurence.logisland.stream.StreamContext;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;

public class LogislandPipelineProcessor extends AbstractProcessor<byte[], byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(LogislandPipelineProcessor.class);

    private RecordSerializer serializer;
    private RecordSerializer deserializer;


    private final StreamContext streamContext;
    private ProcessorContext kafkaProcessContext;

    public LogislandPipelineProcessor(StreamContext streamContext) {
        this.streamContext = streamContext;
    }

    /**
     * build a serializer
     *
     * @param inSerializerClass the serializer type
     * @param schemaContent     an Avro schema
     * @return the serializer
     */
    private RecordSerializer buildSerializer(String inSerializerClass, String schemaContent) {
        return SerializerProvider.getSerializer(inSerializerClass, schemaContent);
    }


    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.kafkaProcessContext = context;
        try {
            ControllerServiceLookup controllerServiceLookup = streamContext.getControllerServiceLookup();
            for (ProcessContext processContext : streamContext.getProcessContexts()) {
                if (processContext.getProcessor().hasControllerService()) {
                    processContext.setControllerServiceLookup(controllerServiceLookup);
                }
                processContext.getProcessor().init(processContext);
            }
            //now init serializers
            if (streamContext.getPropertyValue(StreamProperties.READ_TOPICS_SERIALIZER).asString().equals(StreamProperties.NO_SERIALIZER.getValue())) {
                deserializer = null;
            } else {
                deserializer = buildSerializer(streamContext.getPropertyValue(StreamProperties.READ_TOPICS_SERIALIZER).asString(),
                        streamContext.getPropertyValue(StreamProperties.AVRO_INPUT_SCHEMA).asString());
            }
            serializer = buildSerializer(streamContext.getPropertyValue(StreamProperties.WRITE_TOPICS_SERIALIZER).asString(),
                    streamContext.getPropertyValue(StreamProperties.AVRO_OUTPUT_SCHEMA).asString());


        } catch (InitializationException ie) {
            throw new IllegalStateException("Unable to initialize processor pipeline", ie);
        }


    }

    @Override
    public void process(byte[] key, byte[] value) {
        Record record = null;
        if (deserializer != null) {
            try {
                record = deserializer.deserialize(new ByteArrayInputStream(value));

            } catch (Exception e) {
                logger.error("Unable to serialize record: {}", e.getMessage());
            }
        } else {
            String ks = key != null ? new String(key) : "";
            String vs = value != null ? new String(value) : "";
            record = RecordUtils.getKeyValueRecord(ks, vs);
        }

        if (record != null) {
            try {
                Collection<Record> r = Collections.singleton(record);
                for (ProcessContext processContext : streamContext.getProcessContexts()) {
                    r = processContext.getProcessor().process(processContext, r);
                }
                for (Record out : r) {
                    byte[] k = null;

                    if (out.hasField(FieldDictionary.RECORD_KEY)) {
                        k = out.getField(FieldDictionary.RECORD_KEY).asString().getBytes();
                    }
                    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
                    serializer.serialize(byteOutputStream, out);
                    kafkaProcessContext.forward(k, byteOutputStream.toByteArray());
                }
            } catch (Exception e) {
                logger.error("Unhandled error occurred while executing pipeline {}: {}",
                        streamContext.getStream().getIdentifier(), e.getMessage());
            }
        }
    }
}
