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
package com.hurence.logisland.component;

import com.hurence.logisland.classloading.PluginLoader;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.stream.RecordStream;
import com.hurence.logisland.stream.StandardStreamContext;
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


public final class ComponentFactory {

    private static Logger logger = LoggerFactory.getLogger(ComponentFactory.class);

    private static final AtomicLong currentId = new AtomicLong(0);


    public static Optional<EngineContext> getEngineContext(EngineConfiguration configuration) {
        try {
            final ProcessingEngine engine = loadComponent(configuration.getComponent());
            final EngineContext engineContext =
                    new StandardEngineContext(engine, Long.toString(currentId.incrementAndGet()));


            // instantiate each related pipelineContext
            configuration.getStreamConfigurations().forEach(pipelineConfig -> {
                Optional<StreamContext> pipelineContext = getStreamContext(pipelineConfig);
                pipelineContext.ifPresent(engineContext::addStreamContext);
            });

            configuration.getConfiguration()
                    .forEach((key, value) -> engineContext.setProperty(key, value));


            // load all controller service initialization context
            configuration.getControllerServiceConfigurations()
                    .forEach(controllerServiceConfiguration -> {
                        try {
                            loadComponent(controllerServiceConfiguration.getComponent());
                            engineContext.addControllerServiceConfiguration(controllerServiceConfiguration);

                        } catch (ClassNotFoundException e) {
                            throw new IllegalStateException("unable to instantiate service " + controllerServiceConfiguration.getControllerService() +
                                    ". Please check that your configuration is correct and you installed all the required modules", e);
                        }
                    });

            ((AbstractConfigurableComponent) engine).init(engineContext);

            logger.info("created engine {} with id {}", configuration.getComponent(), currentId.get());


            return Optional.of(engineContext);

        } catch (ClassNotFoundException | InitializationException e) {
            throw new IllegalStateException("unable to instantiate engine " + configuration.getComponent(), e);
        }
    }

    /**
     * Instanciates a stream from of configuration
     *
     * @param configuration
     * @return
     */
    private static Optional<StreamContext> getStreamContext(StreamConfiguration configuration) {
        try {
            final RecordStream recordStream = loadComponent(configuration.getComponent());
            final StreamContext instance =
                    new StandardStreamContext(recordStream, configuration.getStream());

            // instantiate each related processor
            configuration.getProcessorConfigurations().forEach(processConfig -> {
                Optional<ProcessContext> processorContext = getProcessContext(processConfig);
                processorContext.ifPresent(instance::addProcessContext);
            });

            // set the config properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> instance.setProperty(e.getKey(), e.getValue()));
            logger.info("created stream {} with id {}", configuration.getComponent(), configuration.getStream());
            return Optional.of(instance);

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate stream " + configuration.getStream() +
                    ". Please check that your configuration is correct and you installed all the required modules", e);
        }
    }
    //TODO this method should be made private in my opinion (but unfortunately some test class are using it)
    public static Optional<ProcessContext> getProcessContext(ProcessorConfiguration configuration) {
        try {
            final Processor processor = loadComponent(configuration.getComponent());
            final ProcessContext processContext =
                    new StandardProcessContext(processor, configuration.getProcessor());

            // set all properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> processContext.setProperty(e.getKey(), e.getValue()));

            logger.info("Created processor {} with id {}", configuration.getComponent(), configuration.getProcessor());
            return Optional.of(processContext);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate processor " + configuration.getProcessor() +
                    ". Please check that your configuration is correct and you installed all the required modules", e);
        }

    }

    public static <T> T loadComponent(String className) throws ClassNotFoundException {
        //first look for a plugin
        try {
            try {
                return (T) PluginLoader.loadPlugin(className);
            } catch (ClassNotFoundException cnfe) {
                return (T) Class.forName(className).newInstance();
            }
        } catch (Exception e) {
            throw new ClassNotFoundException("Unable to find class " + className, e);
        }
    }
}
