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

package com.hurence.logisland.engine.spark.remote;

import com.hurence.logisland.config.ControllerServiceConfiguration;
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

import java.util.*;
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
    public Optional<StreamContext> getStreamContext(Stream stream) {
        try {
            final RecordStream recordStream =
                    (RecordStream) Class.forName(stream.getComponent()).newInstance();
            final StreamContext instance =
                    new StandardStreamContext(recordStream, stream.getName());

            // instantiate each related processor
            stream.getPipeline().getProcessors().forEach(processor -> {
                Optional<ProcessContext> processorContext = getProcessContext(processor);
                if (processorContext.isPresent())
                    instance.addProcessContext(processorContext.get());
            });


            // set the config properties
            stream.getConfig().forEach(e -> instance.setProperty(e.getKey(), e.getValue()));


            logger.info("created stream {}", stream.getName());
            return Optional.of(instance);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instantiate stream " + stream.getName(), e);
        }
        return Optional.empty();
    }

    /**
     * Constructs processors.
     *
     * @param processor the processor bean.
     * @return optionally the constructed processor context or nothing in case of error.
     */
    public Optional<ProcessContext> getProcessContext(Processor processor) {
        try {
            final com.hurence.logisland.processor.Processor processorInstance =
                    (com.hurence.logisland.processor.Processor) Class.forName(processor.getComponent()).newInstance();
            final ProcessContext processContext =
                    new StandardProcessContext(processorInstance, processor.getName());

            // set all properties
            processor.getConfig().forEach(e -> processContext.setProperty(e.getKey(), e.getValue()));

            logger.info("created processor {}", processor);
            return Optional.of(processContext);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instantiate processor " + processor.getComponent(), e);
        }

        return Optional.empty();
    }


    /**
     * Constructs controller services.
     *
     * @param service the service bean.
     * @return optionally the constructed service configuration or nothing in case of error.
     */
    public Optional<ControllerServiceConfiguration> getControllerServiceConfiguration(Service service) {
        try {
            ControllerServiceConfiguration configuration = new ControllerServiceConfiguration();
            configuration.setControllerService(service.getName());
            configuration.setComponent(service.getComponent());
            configuration.setDocumentation(service.getDocumentation());
            configuration.setType("service");
            configuration.setConfiguration(service.getConfig().stream()
                    .collect(Collectors.toMap(Property::getKey, Property::getValue)));

            logger.info("created service {}", service.getName());
            return Optional.of(configuration);
        } catch (Exception e) {
            logger.error("unable to configure service " + service.getComponent(), e);
        }

        return Optional.empty();
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
            engineContext.getEngine().reset(engineContext);
            logger.info("Configuring dataflow. Last change at {} is {}", dataflow.getLastModified(), dataflow.getModificationReason());
            dataflow.getServices().stream()
                    .map(this::getControllerServiceConfiguration)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(engineContext::addControllerServiceConfiguration);
            dataflow.getStreams().stream()
                    .map(this::getStreamContext)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(engineContext::addStreamContext);
            logger.info("Restarting engine");
            try {
                PipelineConfigurationBroadcastWrapper.getInstance().refresh(
                        engineContext.getStreamContexts().stream()
                                .collect(Collectors.toMap(StreamContext::getIdentifier, StreamContext::getProcessContexts))
                        , sparkContext);
                updatePipelines(sparkContext, engineContext, dataflow.getStreams());
                engineContext.getEngine().start(engineContext);
            } catch (Exception e) {
                logger.error("Unable to start engine. Logisland state may be inconsistent. Trying to recover. Caused by", e);
                engineContext.getEngine().reset(engineContext);
            }
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
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(Collectors.toList())));
        engineContext.getStreamContexts().forEach(streamContext -> {
            streamContext.getProcessContexts().clear();
            streamContext.getProcessContexts().addAll(pipelineMap.get(streamContext.getIdentifier()));
        });
        PipelineConfigurationBroadcastWrapper.getInstance().refresh(pipelineMap, sparkContext);
    }


}