/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.connect;

import com.hurence.logisland.stream.spark.StreamOptions;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {


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

    public static Converter createConverter(String converterClassName, String propertiesAsString, boolean isKey)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {
        Converter ret = (Converter) Class.forName(converterClassName).newInstance();
        ret.configure(propertiesToMap(propertiesAsString), isKey);
        return ret;
    }

    public static java.util.Map<String, String> propertiesToMap(String propertiesAsString) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propertiesAsString));
        return props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
    }

    public static OffsetBackingStore createOffsetBackingStore(String type, Map<String, String> properties) {
        WorkerConfig workerConfig = null;
        OffsetBackingStore offsetBackingStore;
        if (StreamOptions.FILE_BACKING_STORE().getValue().equals(type)) {
            offsetBackingStore = new FileOffsetBackingStore();
            workerConfig = new FileConfig(properties);
        } else if (StreamOptions.MEMORY_BACKING_STORE().getValue().equals(type)) {
            offsetBackingStore = new MemoryOffsetBackingStore();
            workerConfig = new MemoryConfig(properties);
        } else if (StreamOptions.KAFKA_BACKING_STORE().getValue().equals(type)) {
            offsetBackingStore = new KafkaOffsetBackingStore();
            workerConfig = new KafkaConfig(properties);
        } else {
            throw new IllegalArgumentException(StreamOptions.KAFKA_CONNECT_OFFSET_BACKING_STORE().getName() +
                    " must be set!");
        }
        offsetBackingStore.configure(workerConfig);
        return offsetBackingStore;
    }

}
