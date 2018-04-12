/*
 * Copyright (C) 2018 Hurence (support@hurence.com)
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
 *
 */

package com.hurence.logisland.connect.source;

import com.hurence.logisland.stream.spark.StreamOptions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A {@link StreamSourceProvider} capable of creating spark {@link com.hurence.logisland.stream.spark.structured.StructuredStream}
 * enabled kafka sources.
 *
 * @author amarziali
 */
public class KafkaConnectStreamSourceProvider implements StreamSourceProvider {

    /**
     * Configuration definition for {@link MemoryOffsetBackingStore}
     */
    private static class MemoryConfig extends WorkerConfig {
        public MemoryConfig(java.util.Map<String, String> props) {
            super(new ConfigDef(), props);
        }
    }

    /**
     * Configuration definition for {@link FileOffsetBackingStore}
     */
    private static class FileConfig extends WorkerConfig {
        public FileConfig(java.util.Map<String, String> props) {
            super(new ConfigDef()
                            .define(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    "file to store offset data in")
                    , props);
        }
    }

    /**
     * Configuration definition for {@link KafkaOffsetBackingStore}
     */
    private static class KafkaConfig extends WorkerConfig {
        public KafkaConfig(java.util.Map<String, String> props) {
            super(new ConfigDef()
                            .define(BOOTSTRAP_SERVERS_CONFIG,
                                    ConfigDef.Type.LIST,
                                    BOOTSTRAP_SERVERS_DEFAULT,
                                    ConfigDef.Importance.HIGH,
                                    BOOTSTRAP_SERVERS_DOC)
                            .define(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG,
                                    ConfigDef.Type.STRING,
                                    ConfigDef.Importance.HIGH,
                                    "kafka topic to store connector offsets in")
                    , props);
        }
    }

    private Converter createConverter(Map<String, String> parameters, String classKey, String propertyKey, boolean isKey)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        Converter ret = (Converter) Class.forName(parameters.get(classKey).get()).newInstance();
        ret.configure(propertiesToMap(parameters.get(propertyKey).get()), isKey);
        return ret;
    }

    private java.util.Map<String, String> propertiesToMap(String propertiesAsString) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propertiesAsString));
        return props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        try {
            Converter keyConverter = createConverter(parameters, StreamOptions.KAFKA_CONNECT_KEY_CONVERTER().getName(),
                    StreamOptions.KAFKA_CONNECT_KEY_CONVERTER_PROPERTIES().getName(), true);
            Converter valueConverter = createConverter(parameters, StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER().getName(),
                    StreamOptions.KAFKA_CONNECT_VALUE_CONVERTER_PROPERTIES().getName(), false);
            //create the right backing store
            OffsetBackingStore offsetBackingStore = null;
            WorkerConfig workerConfig = null;
            java.util.Map<String, String> offsetBackingStoreProperties =
                    propertiesToMap(parameters.get(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE_PROPERTIES().getName()).get());
            String bs = parameters.get(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE().getName()).get();
            if (StreamOptions.FILE_BACKING_STORE().getValue().equals(bs)) {
                offsetBackingStore = new FileOffsetBackingStore();
                workerConfig = new FileConfig(offsetBackingStoreProperties);
            } else if (StreamOptions.MEMORY_BACKING_STORE().getValue().equals(bs)) {
                offsetBackingStore = new MemoryOffsetBackingStore();
                workerConfig = new MemoryConfig(offsetBackingStoreProperties);
            } else if (StreamOptions.KAFKA_BACKING_STORE().getValue().equals(bs)) {
                offsetBackingStore = new KafkaOffsetBackingStore();
                workerConfig = new KafkaConfig(offsetBackingStoreProperties);
            } else {
                throw new IllegalArgumentException(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE().getName() +
                        " must be set!");
            }

            offsetBackingStore.configure(workerConfig);
            return new KafkaConnectStreamSource(sqlContext,
                    propertiesToMap(parameters.get(StreamOptions.KAFKA_CONNECT_CONNECTOR_PROPERTIES().getName()).get()),
                    keyConverter,
                    valueConverter,
                    offsetBackingStore,
                    Integer.parseInt(parameters.get(StreamOptions.KAFKA_CONNECT_MAX_TASKS().getName()).get()),
                    (Class<? extends SourceConnector>) Class.forName(parameters.get(StreamOptions.KAFKA_CONNECT_CONNECTOR_CLASS().getName()).get()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to create kafka connect stream source: " + e.getMessage(), e);
        }


    }

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema, String providerName, Map<String, String> parameters) {
        return Tuple2.<String, StructType>apply(providerName, KafkaConnectStreamSource.DATA_SCHEMA);
    }
}
