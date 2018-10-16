/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
 */
package com.hurence.logisland.engine.spark.remote;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ConfigurableComponent;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.controller.ControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.controller.StandardControllerServiceContext;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.remote.model.*;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.stream.RecordStream;
import com.hurence.logisland.stream.StandardStreamContext;
import com.hurence.logisland.stream.StreamContext;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ]
 * Component factory resolving logisland components from remote api model.
 *
 * @author amarziali
 */
public class RemoteApiComponentFactory {

    private static final Logger logger = LoggerFactory.getLogger(RemoteApiComponentFactory.class);


    /**
     * Instantiates a stream from of configuration
     *
     * @param stream
     * @return
     */
    public StreamContext getStreamContext(Stream stream) {
        try {
            final RecordStream recordStream = ComponentFactory.loadComponent(stream.getComponent());
            final StreamContext instance =
                    new StandardStreamContext(recordStream, stream.getName());

            // instantiate each related processor
            stream.getPipeline().getProcessors().stream()
                    .map(this::getProcessContext)
                    .forEach(instance::addProcessContext);


            // set the config properties
            configureComponent(recordStream, stream.getConfig())
                    .forEach((k, s) -> instance.setProperty(k, s));
            if (!instance.isValid()) {
                throw new IllegalArgumentException("Stream is not valid");
            }

            logger.info("created stream {}", stream.getName());
            return instance;

        } catch (ClassNotFoundException e) {
            throw new RuntimeException("unable to instantiate stream " + stream.getName(), e);
        }
    }

    /**
     * Constructs processors.
     *
     * @param processor the processor bean.
     * @return optionally the constructed processor context or nothing in case of error.
     */
    public ProcessContext getProcessContext(Processor processor) {
        try {
            final com.hurence.logisland.processor.Processor processorInstance = ComponentFactory.loadComponent(processor.getComponent());
            final ProcessContext processContext =
                    new StandardProcessContext(processorInstance, processor.getName());

            // set all properties
            configureComponent(processorInstance, processor.getConfig())
                    .forEach((k, s) -> processContext.setProperty(k, s));
            ;

            if (!processContext.isValid()) {
                throw new IllegalArgumentException("Processor is not valid");
            }


            logger.info("created processor {}", processor);
            return processContext;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("unable to instantiate processor " + processor.getName(), e);
        }

    }


    /**
     * Constructs controller services.
     *
     * @param service the service bean.
     * @return optionally the constructed service configuration or nothing in case of error.
     */
    public ControllerServiceConfiguration getControllerServiceConfiguration(Service service) {
        try {
            ControllerService cs = ComponentFactory.loadComponent(service.getComponent());
            ControllerServiceConfiguration configuration = new ControllerServiceConfiguration();
            configuration.setControllerService(service.getName());
            configuration.setComponent(service.getComponent());
            configuration.setDocumentation(service.getDocumentation());
            configuration.setType("service");
            configuration.setConfiguration(configureComponent(cs, service.getConfig()));
            ControllerServiceInitializationContext ic = new StandardControllerServiceContext(cs, service.getName());
            configuration.getConfiguration().forEach((k, s) -> ic.setProperty(k, s));
            if (!ic.isValid()) {
                throw new IllegalArgumentException("Service is not valid");
            }
            logger.info("created service {}", service.getName());
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException("unable to instantiate service " + service.getName(), e);
        }


    }

    /**
     * Updates the state of the engine if needed.
     *
     * @param sparkContext  the spark context
     * @param engineContext the engineContext
     * @param dataflow      the new dataflow (new state)
     * @param oldDataflow   latest dataflow dataflow.
     */
    public boolean updateEngineContext(SparkContext sparkContext, EngineContext engineContext, DataFlow dataflow, DataFlow oldDataflow) {
        boolean changed = false;
        if (oldDataflow == null || oldDataflow.getLastModified().isBefore(dataflow.getLastModified())) {
            logger.info("We have a new configuration. Resetting current engine");
            logger.info("Configuring dataflow. Last change at {} is {}", dataflow.getLastModified(), dataflow.getModificationReason());


            List<ControllerServiceConfiguration> css = dataflow.getServices().stream()
                    .map(this::getControllerServiceConfiguration)
                    .collect(Collectors.toList());

            List<StreamContext> sc = dataflow.getStreams().stream()
                    .map(this::getStreamContext)
                    .collect(Collectors.toList());

            sc.forEach(streamContext -> {
                if (!streamContext.isValid()) {
                    throw new IllegalArgumentException("Unable to validate steam " + streamContext.getIdentifier());
                }
            });

            logger.info("Restarting engine");
            engineContext.getEngine().reset(engineContext);
            css.forEach(engineContext::addControllerServiceConfiguration);
            sc.forEach(engineContext::addStreamContext);

            PipelineConfigurationBroadcastWrapper.getInstance().refresh(
                    engineContext.getStreamContexts().stream()
                            .collect(Collectors.toMap(StreamContext::getIdentifier, StreamContext::getProcessContexts))
                    , sparkContext);
            updatePipelines(sparkContext, engineContext, dataflow.getStreams());
            engineContext.getEngine().start(engineContext);
            changed = true;

        } else {
            //need to update pipelines?

            Map<String, Stream> streamMap = dataflow.getStreams().stream().collect(Collectors.toMap(Stream::getName, Function.identity()));

            List<Stream> mergedStreamList = new ArrayList<>();
            for (Stream oldStream : oldDataflow.getStreams()) {
                Stream newStream = streamMap.get(oldStream.getName());
                if (newStream != null && oldStream.getPipeline().getLastModified().isBefore(newStream.getPipeline().getLastModified())) {
                    changed = true;
                    logger.info("Detected change for pipeline {}", newStream.getName());
                    mergedStreamList.add(newStream);
                } else {
                    mergedStreamList.add(oldStream);
                }
            }
            if (changed) {
                updatePipelines(sparkContext, engineContext, mergedStreamList);
            }

        }
        return changed;
    }


    /**
     * Update pipelines.
     *
     * @param sparkContext  the spark context
     * @param engineContext the engine context.
     * @param streams       the list of streams
     */
    public void updatePipelines(SparkContext sparkContext, EngineContext engineContext, Collection<Stream> streams) {
        Map<String, Collection<ProcessContext>> pipelineMap = streams.stream()
                .collect(Collectors.toMap(Stream::getName,
                        s -> s.getPipeline().getProcessors().stream().map(this::getProcessContext)
                                .collect(Collectors.toList())));
        engineContext.getStreamContexts().forEach(streamContext -> {
            streamContext.getProcessContexts().clear();
            streamContext.getProcessContexts().addAll(pipelineMap.get(streamContext.getIdentifier()));
        });

        PipelineConfigurationBroadcastWrapper.getInstance().refresh(pipelineMap, sparkContext);
    }

    private Map<String, String> configureComponent(ConfigurableComponent component, Collection<Property> properties) {
        final Map<String, Property> propertyMap = properties.stream().collect(Collectors.toMap(Property::getKey, Function.identity()));
        return propertyMap.keySet().stream().map(component::getPropertyDescriptor)
                .filter(propertyDescriptor -> propertyDescriptor != null)
                .filter(propertyDescriptor -> propertyMap.containsKey(propertyDescriptor.getName()) ||
                        (propertyDescriptor.getDefaultValue() != null && propertyDescriptor.isRequired()))
                .collect(Collectors.toMap(PropertyDescriptor::getName, propertyDescriptor -> {
                    String value = propertyDescriptor.getDefaultValue();
                    if (propertyMap.containsKey(propertyDescriptor.getName())) {
                        value = propertyMap.get(propertyDescriptor.getName()).getValue();
                    }
                    return value;
                }));
    }


}