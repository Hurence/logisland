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

import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.remote.model.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Stateful component registry.
 *
 * @author amarziali
 */
public class RemoteComponentRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RemoteComponentRegistry.class);

    private final EngineContext engineContext;
    private final Map<String, EngineContext> childContextes = new HashMap<>();
    private final Map<String, Pipeline> pipelineMap = new HashMap<>();

    private final RemoteApiComponentFactory remoteApiComponentFactory = new RemoteApiComponentFactory();

    /**
     * Create an instance.
     *
     * @param engineContext the master engine (initial state).
     */
    public RemoteComponentRegistry(EngineContext engineContext) {
        this.engineContext = engineContext;
    }

    private void stopPipeline(Pipeline pipeline) {
        EngineContext ctx = childContextes.remove(pipeline.getName());
        pipelineMap.remove(pipeline.getName());
        logger.info("Stopping everything for pipeline {}", ctx.getName());
        ctx.getStreamContexts().forEach(streamContext -> {
            logger.info("Pipeline {} : stopping stream {}", pipeline.getName(), streamContext.getName());
            try {
                streamContext.getStream().stop();
                logger.info("Pipeline {} : successfully stopped stream {}", pipeline.getName(), streamContext.getName());
            } catch (Exception e) {
                logger.error("Pipeline {} : unexpected error stopping stream {}: {}", pipeline.getName(), streamContext.getName(), e.getMessage());
            }
        });
        try {
            engineContext.close();
        } catch (IOException e) {
            logger.warn("Unable to properly close engine " + engineContext.getName(), e);
        }
        logger.info("Pipeline {} stopped", ctx.getName());

    }

    private void startPipeline(Pipeline pipeline) {
        {
            logger.info("Creating engine for pipeline {}", pipeline.getName());
            //create a new scoped engine context corresponding to the pipeline
            EngineContext ec = remoteApiComponentFactory.createScopedEngineContext(engineContext, pipeline);
            //add every service controller it needs
            pipeline.getServices().stream()
                    .map(remoteApiComponentFactory::getControllerServiceConfiguration)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(ec::addControllerServiceConfiguration);
            //now instantiate every stream
            pipeline.getStreams().stream()
                    .map(remoteApiComponentFactory::getStreamContext)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(ec::addStreamContext);
            //start it
            try {
                logger.info("Starting engine for pipeline {}", pipeline.getName());
                ec.getEngine().start(ec);
                //now add the engine context to the registry
                childContextes.put(pipeline.getName(), ec);
                pipelineMap.put(pipeline.getName(), pipeline);
                logger.info("Pipeline {} successfully started", pipeline.getName());
            } catch (Exception e) {
                logger.error("Unable to properly start pipeline " + pipeline.getName(), e);
                try {
                    logger.info("Shutting down pipeline {}", pipeline.getName());
                    ec.getEngine().shutdown(ec);
                    logger.info("Pipeline {} successfully shut down", pipeline.getName());
                } catch (Exception e1) {
                    logger.warn("Unable to properly shut down pipeline " + pipeline.getName(), e1);
                }
            }
        }
    }


    private Optional<Pipeline> findInPipelines(Collection<Pipeline> list, Pipeline item) {
        return list.stream().filter(p -> p.getName().equals(item.getName())).findFirst();
    }


    /**
     * Updates the state of the engine by adding / removing pipelines according to the new provided config.
     *
     * @param pipelines the list of pipelines (new state)
     */
    public void updateEngineContext(Collection<Pipeline> pipelines) {
        //remove missing or outdated items
        pipelineMap.values().stream()
                .filter(pipeline -> {
                    Optional<Pipeline> found = findInPipelines(pipelines, pipeline);
                    return !found.isPresent() || pipeline.getLastModified().isBefore(found.get().getLastModified());
                })
                .collect(Collectors.toList())
                //remove active streams inside this pipeline
                .forEach(this::stopPipeline);

        //add new items
        pipelines.stream()
                .filter(pipeline -> !pipelineMap.containsKey(pipeline.getName()))
                .collect(Collectors.toList())
                .forEach(this::startPipeline);

    }
}
