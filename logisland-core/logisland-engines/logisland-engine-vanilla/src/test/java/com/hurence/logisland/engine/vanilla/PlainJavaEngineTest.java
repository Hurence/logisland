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

package com.hurence.logisland.engine.vanilla;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.vanilla.stream.kafka.KafkaParallelProcessingStream;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class PlainJavaEngineTest {


    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();


    private EngineConfiguration engineConfiguration() {
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setType("engine");
        engineConfiguration.setDocumentation("Plain java engine");
        engineConfiguration.setComponent(PlainJavaEngine.class.getCanonicalName());
        return engineConfiguration;
    }

    private final Supplier<Map<String, String>> defaultPropertySupplier(Map<String, String> props) {
        return () -> {
            Map<String, String> conf = new HashMap<>(props);
            conf.put("application.id", "test");
            conf.put("bootstrap.servers", sharedKafkaTestResource.getKafkaConnectString());
            return conf;
        };
    }

    private final Supplier<Map<String, String>> defaultPropertySupplier() {
        return defaultPropertySupplier(Collections.emptyMap());
    }

    private StreamConfiguration emptyKafkaStream(Supplier<Map<String, String>> propertySupplier) {
        StreamConfiguration streamConfiguration = new StreamConfiguration();
        streamConfiguration.setStream("kafka_empty");
        streamConfiguration.setComponent(KafkaParallelProcessingStream.class.getCanonicalName());

        streamConfiguration.setType("stream");
        streamConfiguration.setConfiguration(propertySupplier.get());
        return streamConfiguration;
    }

    @Test
    public void testEmpty() {
        EngineContext engineContext = ComponentFactory.getEngineContext(engineConfiguration()).get();
        engineContext.getEngine().start(engineContext);
        engineContext.getEngine().awaitTermination(engineContext);
        engineContext.getEngine().shutdown(engineContext);
    }

    @Test
    public void testWithDefaultKafkaStream() {
        EngineConfiguration engineConfiguration = engineConfiguration();
        engineConfiguration.addPipelineConfigurations(emptyKafkaStream(defaultPropertySupplier()));
        EngineContext engineContext = ComponentFactory.getEngineContext(engineConfiguration).get();
        engineContext.getEngine().start(engineContext);
        Assert.assertTrue(engineContext.isValid());
        engineContext.getEngine().shutdown(engineContext);
        engineContext.getEngine().awaitTermination(engineContext);
    }

    @Test
    public void testWithBadConfiguredKafkaStream() {
        EngineConfiguration engineConfiguration = engineConfiguration();
        engineConfiguration.addPipelineConfigurations(emptyKafkaStream(defaultPropertySupplier(Collections.singletonMap("i.do.not.exist", "!"))));
        EngineContext engineContext = ComponentFactory.getEngineContext(engineConfiguration).get();
        engineContext.getEngine().start(engineContext);
        Assert.assertFalse(engineContext.isValid());
        engineContext.getEngine().shutdown(engineContext);
        engineContext.getEngine().awaitTermination(engineContext);
    }

}