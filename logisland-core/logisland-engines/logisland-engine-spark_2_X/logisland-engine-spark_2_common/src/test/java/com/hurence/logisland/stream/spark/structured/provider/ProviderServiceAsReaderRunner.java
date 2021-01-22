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
package com.hurence.logisland.stream.spark.structured.provider;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
//import com.hurence.logisland.processor.DebugStream;
import com.hurence.logisland.stream.StreamProperties;
import com.hurence.logisland.stream.spark.structured.StructuredStream;
import com.hurence.logisland.util.runner.MockControllerServiceLookup;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ProviderServiceAsReaderRunner {

    private static Logger logger = LoggerFactory.getLogger(ProviderServiceAsReaderRunner.class);

    private final StructuredStreamProviderServiceReader provider;
    private final MockControllerServiceLookup serviceLookup;


    public ProviderServiceAsReaderRunner(StructuredStreamProviderServiceReader provider) {
        this.provider = provider;
        this.serviceLookup = new MockControllerServiceLookup();
    }

    public void run() {
        EngineContext engineContext = ComponentFactory.getEngineContext(getEngineConfiguration()).get();
        Assert.assertTrue(engineContext.isValid());
        try {
            engineContext.getEngine().start(engineContext);
            engineContext.getEngine().awaitTermination(engineContext);
        } catch (Exception ex) {
            engineContext.getEngine().stop(engineContext);
        }
    }

    private EngineConfiguration getEngineConfiguration() {
        EngineConfiguration engineConfiguration = new EngineConfiguration();
        engineConfiguration.setType("engine");
        engineConfiguration.setDocumentation("Plain java engine");
        engineConfiguration.setComponent(KafkaStreamProcessingEngine.class.getCanonicalName());
        Map<String, String> props = new HashMap<>();
        props.put(StreamProperties.READ_TOPICS_SERIALIZER().getName(), "none");
        props.put(StreamProperties.READ_STREAM_SERVICE_PROVIDER().getName(), "local_file_service");
        props.put(StreamProperties.WRITE_TOPICS_SERIALIZER().getName(), StreamProperties.JSON_SERIALIZER().getValue());
        props.put(StreamProperties.WRITE_STREAM_SERVICE_PROVIDER().getName(), "console_service");
        StreamConfiguration streamConfiguration = testStructuredStreamStream(props);
//        streamConfiguration.addProcessorConfiguration(debugProcessorConfiguration(Collections.emptyMap()));
        engineConfiguration.addPipelineConfigurations(streamConfiguration);
        //set up services
        Map<String, String> propsFileProvider = new HashMap<>();
        propsFileProvider.put("local.input.path", getClass().getResource("/input").getFile());
        List<ControllerServiceConfiguration> services = new ArrayList<>();
        services.add(testLocalFileProvider(propsFileProvider));

        Map<String, String> propsConsoleProvider = new HashMap<>();
        propsConsoleProvider.put("truncate", "false");
        services.add(testConsoleProvider(propsConsoleProvider));
        engineConfiguration.setControllerServiceConfigurations(services);
        return engineConfiguration;
    }

    private StreamConfiguration testStructuredStreamStream(Map<String, String> props) {
        StreamConfiguration streamConfiguration = new StreamConfiguration();
        streamConfiguration.setStream("testStructuredStream");
        streamConfiguration.setComponent(StructuredStream.class.getCanonicalName());
        streamConfiguration.setType("stream");
        streamConfiguration.setConfiguration(props);
        return streamConfiguration;
    }

    private ControllerServiceConfiguration testLocalFileProvider(Map<String, String> props) {
        ControllerServiceConfiguration serviceConfiguration = new ControllerServiceConfiguration();
        serviceConfiguration.setControllerService("local_file_service");
        serviceConfiguration.setComponent(LocalFileStructuredStreamProviderService.class.getCanonicalName());
        serviceConfiguration.setType("provider");
        serviceConfiguration.setConfiguration(props);
        return serviceConfiguration;
    }

    private ControllerServiceConfiguration testConsoleProvider(Map<String, String> props) {
        ControllerServiceConfiguration serviceConfiguration = new ControllerServiceConfiguration();
        serviceConfiguration.setControllerService("console_service");
        serviceConfiguration.setComponent(ConsoleStructuredStreamProviderService.class.getCanonicalName());
        serviceConfiguration.setType("provider");
        serviceConfiguration.setConfiguration(props);
        return serviceConfiguration;
    }

    private ProcessorConfiguration debugProcessorConfiguration(Map<String, String> props) {
        ProcessorConfiguration ret = new ProcessorConfiguration();
        ret.setProcessor(UUID.randomUUID().toString());
//        ret.setComponent(DebugStream.class.getCanonicalName());
        ret.setType("processor");
        return ret;
    }


}
