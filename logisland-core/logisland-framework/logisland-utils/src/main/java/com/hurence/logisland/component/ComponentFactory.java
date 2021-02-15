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
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.controller.StandardControllerServiceContext;
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


    public static Optional<EngineContext> buildAndSetUpEngineContext(EngineConfiguration engineConfig) {
        try {
            final ProcessingEngine engine = loadComponent(engineConfig.getComponent());
            final EngineContext engineContext =
                    new StandardEngineContext(engine, Long.toString(currentId.incrementAndGet()));
            if (engine instanceof AbstractConfigurableComponent) {
                ((AbstractConfigurableComponent) engine).setIdentifier(String.valueOf(currentId.get()));
            }

            // instantiate each related pipelineContext
            engineConfig.getStreamConfigurations().forEach(streamConfig -> {
                StreamContext streamContextOpt = buildAndSetUpStreamContext(streamConfig);
                engineContext.addStreamContext(streamContextOpt);
            });

            // load all controller service initialization context
            engineConfig.getControllerServiceConfigurations().forEach(controllerServiceConfiguration -> {
                ControllerServiceInitializationContext serviceContext = buildAndSetUpServiceContext(controllerServiceConfiguration);
                engineContext.addControllerServiceContext(serviceContext);
            });

            engineConfig.getConfiguration()
                    .forEach((key, value) -> engineContext.setProperty(key, value));

            engineContext.getLogger().info("Engine is now configured");
            logger.info("created engine {} with id {}", engineConfig.getComponent(), currentId.get());

            return Optional.of(engineContext);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate engine " + engineConfig.getComponent(), e);
        }
    }

    /**
     * Instanciates a stream from of configuration
     *
     * @param configuration
     * @return
     */
    public static StreamContext buildAndSetUpStreamContext(StreamConfiguration configuration) {
        try {
            final RecordStream recordStream = loadComponent(configuration.getComponent());
            final StreamContext streamContext =
                    new StandardStreamContext(recordStream, configuration.getStream());
            if (recordStream instanceof AbstractConfigurableComponent) {
                ((AbstractConfigurableComponent) recordStream).setIdentifier(configuration.getStream());
            }
            // instantiate each related processor
            configuration.getProcessorConfigurations().forEach(processConfig -> {
                ProcessContext processorContext = buildAndSetUpProcessContext(processConfig);
                streamContext.addProcessContext(processorContext);
            });

            // set the config properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> streamContext.setProperty(e.getKey(), e.getValue()));

            streamContext.getLogger().info("Stream is now configured");
            logger.info("created stream {} with id {}", configuration.getComponent(), configuration.getStream());
            return streamContext;

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate stream " + configuration.getStream() +
                    ". Please check that your configuration is correct and you installed all the required modules", e);
        }
    }

    public static ProcessContext buildAndSetUpProcessContext(ProcessorConfiguration configuration) {
        try {
            logger.info("Setting up processor {} with id {}", configuration.getComponent(), configuration.getProcessor());
            final Processor processor = loadComponent(configuration.getComponent());
            if (processor instanceof AbstractConfigurableComponent) {
                ((AbstractConfigurableComponent) processor).setIdentifier(configuration.getProcessor());
            }
            final ProcessContext processContext =
                    new StandardProcessContext(processor, configuration.getProcessor());
            // set all properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> processContext.setProperty(e.getKey(), e.getValue()));

            processContext.getLogger().info("Processor is now configured");
            logger.info("Created processor {} with id {}", configuration.getComponent(), configuration.getProcessor());
            return processContext;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate processor " + configuration.getProcessor() +
                    ". Please check that your configuration is correct and you installed all the required modules", e);
        }
    }

    /**
     * Instanciates a stream from of configuration
     *
     * @param serviceConfig
     * @return
     */
    public static ControllerServiceInitializationContext buildAndSetUpServiceContext(ControllerServiceConfiguration serviceConfig) {
        try {
            final ControllerService service = loadComponent(serviceConfig.getComponent());
            final ControllerServiceInitializationContext serviceContext =
                    new StandardControllerServiceContext(service, serviceConfig.getControllerService());
            if (service instanceof AbstractConfigurableComponent) {
                ((AbstractConfigurableComponent) service).setIdentifier(serviceConfig.getControllerService());
            }
            // set the config properties
            serviceConfig.getConfiguration()
                    .entrySet().forEach(e -> serviceContext.setProperty(e.getKey(), e.getValue()));

            serviceContext.getLogger().info("Service is now configured");
            logger.info("created service {} with id {}", serviceConfig.getComponent(), serviceConfig.getControllerService());
            return serviceContext;
        } catch (IllegalArgumentException | ClassNotFoundException e) {
            throw new IllegalStateException("unable to instantiate service " + serviceConfig.getComponent() +
                    ". Please check that your configuration is correct and you installed all the required modules", e);
        }
    }

    public static <T> T loadComponent(String className) throws ClassNotFoundException {
        //first look for a plugin
        try {
            try {
                return (T) PluginLoader.loadPlugin(className);
            } catch (ClassNotFoundException cnfe) {
                logger.warn("Class " + className + " not found in plugins: trying to load from current class loader");
                return (T) Class.forName(className).newInstance();
            }
        } catch (Exception e) {
            throw new ClassNotFoundException("Unable to find class " + className, e);
        }
    }
}
