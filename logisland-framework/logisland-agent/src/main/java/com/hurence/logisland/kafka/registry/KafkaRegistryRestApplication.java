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
package com.hurence.logisland.kafka.registry;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.kafka.registry.exceptions.RegistryException;
import com.hurence.logisland.kafka.serialization.RegistrySerializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Configurable;
import java.util.Properties;

public class KafkaRegistryRestApplication extends Application<KafkaRegistryConfig> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRegistryRestApplication.class);
    private KafkaRegistry kafkaRegistry = null;

    public KafkaRegistryRestApplication(Properties props) throws RestConfigException {
        this(new KafkaRegistryConfig(props));
    }

    public KafkaRegistryRestApplication(KafkaRegistryConfig config) {
        super(config);
    }

    @Override
    public void setupResources(Configurable<?> config, KafkaRegistryConfig schemaRegistryConfig) {
        try {
            kafkaRegistry = new KafkaRegistry(schemaRegistryConfig,  new RegistrySerializer());
            kafkaRegistry.init();
        } catch (RegistryException e) {
            log.error("Error starting the schema registry", e);
            System.exit(1);
        }

        config.register(new DefaultApi(kafkaRegistry));
        config.register(new JobsApi(kafkaRegistry));
        config.register(new TopicsApi(kafkaRegistry));
        config.register(new ConfigsApi(kafkaRegistry));
        config.register(new ProcessorsApi(kafkaRegistry));
    }

    @Override
    public void onShutdown() {
        kafkaRegistry.close();
    }

    // for testing purpose only
    public KafkaRegistry schemaRegistry() {
        return kafkaRegistry;
    }
}
