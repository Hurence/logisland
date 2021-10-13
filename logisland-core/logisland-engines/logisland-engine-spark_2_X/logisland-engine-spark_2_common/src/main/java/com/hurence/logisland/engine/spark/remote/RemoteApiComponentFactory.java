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
package com.hurence.logisland.engine.spark.remote;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.remote.model.*;
import com.hurence.logisland.processor.ProcessContext;
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
    private StreamContext buildAndSetupStreamContext(Stream stream) {
        StreamConfiguration configuration = new StreamConfiguration();
        configuration.setStream(stream.getName());
        configuration.setComponent(stream.getComponent());
        configuration.setDocumentation(stream.getDocumentation());
        configuration.setType("stream");
        configuration.setConfiguration(propsAsMap(stream.getConfig()));
        stream.getPipeline().getProcessors().stream()
                .map(this::getProcessorConfiguration)
                .forEach(configuration::addProcessorConfiguration);

        return ComponentFactory.buildAndSetUpStreamContext(configuration);
    }

    /**
     * Constructs processors.
     *
     * @param processor the processor bean.
     * @return optionally the constructed processor context or nothing in case of error.
     */
    private ProcessorConfiguration getProcessorConfiguration(Processor processor) {
        ProcessorConfiguration configuration = new ProcessorConfiguration();
        configuration.setProcessor(processor.getName());
        configuration.setComponent(processor.getComponent());
        configuration.setDocumentation(processor.getDocumentation());
        configuration.setType("processor");
        configuration.setConfiguration(propsAsMap(processor.getConfig()));
        return configuration;
    }

    private ProcessContext buildAndSetUpProcessorContext(Processor processor) {
        ProcessorConfiguration configuration = getProcessorConfiguration(processor);
        return ComponentFactory.buildAndSetUpProcessContext(configuration);
    }

    /**
     * Constructs controller services.
     *
     * @param service the service bean.
     * @return optionally the constructed service configuration or nothing in case of error.
     */
    private ControllerServiceInitializationContext buildAndSetUpControllerServiceContext(Service service) {
        ControllerServiceConfiguration configuration = new ControllerServiceConfiguration();
        configuration.setControllerService(service.getName());
        configuration.setComponent(service.getComponent());
        configuration.setDocumentation(service.getDocumentation());
        configuration.setType("service");
        configuration.setConfiguration(propsAsMap(service.getConfig()));
        return ComponentFactory.buildAndSetUpServiceContext(configuration);
    }

    /**
     * Constructs controller services.
     *
     * @param service the service bean.
     * @return optionally the constructed service configuration or nothing in case of error.
     */
    private ControllerServiceConfiguration buildControllerServiceConfiguration(Service service) {
        ControllerServiceConfiguration configuration = new ControllerServiceConfiguration();
        configuration.setControllerService(service.getName());
        configuration.setComponent(service.getComponent());
        configuration.setDocumentation(service.getDocumentation());
        configuration.setType("service");
        configuration.setConfiguration(propsAsMap(service.getConfig()));
        return configuration;
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
            List<ControllerServiceInitializationContext> serviceContexts = dataflow.getServices().stream()
                    .map(this::buildAndSetUpControllerServiceContext)
                    .collect(Collectors.toList());

            List<ControllerServiceConfiguration> serviceConfigurations = dataflow.getServices().stream()
                    .map(this::buildControllerServiceConfiguration)
                    .collect(Collectors.toList());

            List<StreamContext> streamContexts = dataflow.getStreams().stream()
                    .map(this::buildAndSetupStreamContext)
                    .collect(Collectors.toList());

            streamContexts.forEach(streamContext -> {
                if (!streamContext.isValid()) {
                    throw new IllegalArgumentException("configuration of stream " + streamContext.getIdentifier() + " is not valid");
                }
            });
            serviceContexts.forEach(serviceContext -> {
                if (!serviceContext.isValid()) {
                    throw new IllegalArgumentException("configuration of service " + serviceContext.getIdentifier() + " is not valid");
                }
            });

            logger.info("Restarting engine");
            engineContext.getEngine().softStop(engineContext);
            serviceContexts.forEach(engineContext::addControllerServiceContext);
            serviceConfigurations.forEach(engineContext::addControllerServiceConfiguration);

            streamContexts.forEach(engineContext::addStreamContext);


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
    private void updatePipelines(SparkContext sparkContext, EngineContext engineContext, Collection<Stream> streams) {
        Map<String, Collection<ProcessContext>> pipelineMap = streams.stream()
                .collect(Collectors.toMap(Stream::getName,
                        s -> s.getPipeline().getProcessors().stream().map(this::buildAndSetUpProcessorContext)
                                .collect(Collectors.toList())));
        engineContext.getStreamContexts().forEach(streamContext -> {
            streamContext.getProcessContexts().clear();
            streamContext.getProcessContexts().addAll(pipelineMap.get(streamContext.getIdentifier()));
        });

        PipelineConfigurationBroadcastWrapper.getInstance().refresh(pipelineMap, sparkContext);
    }

    private static Map<String, String> propsAsMap(Collection<Property> properties) {
        return properties.stream().collect(
                Collectors.toMap(
                        Property::getKey,
                        Property::getValue
                ));
    }


}