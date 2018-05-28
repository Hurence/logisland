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
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RemoteComponentRegistry {

    private static final Logger logger = LoggerFactory.getLogger(RemoteComponentRegistry.class);

    private final EngineContext engineContext;
    private final Map<Pipeline, List<EngineContext>> registry = new HashMap<>();
    private final RemoteApiComponentFactory remoteApiComponentFactory = new RemoteApiComponentFactory();

    public RemoteComponentRegistry(EngineContext engineContext) {
        this.engineContext = engineContext;
    }

    public void updateEngineContext(Collection<Pipeline> pipelines) {
        //remove missing items
        registry.keySet().stream()
                .filter(pipeline -> !pipelines.contains(pipeline))
                .collect(Collectors.toList())
                //remove active streams inside this pipeline
                .forEach(pipeline -> registry.remove(pipeline).forEach(engineContext -> {
                    logger.info("Pipeline {} : stopping engine {}", pipeline.getName(), engineContext.getName());
                    try {
                        engineContext.getEngine().shutdown(engineContext);
                        logger.info("Pipeline {} : successfully stopped engine {}", pipeline.getName(), engineContext.getName());
                    } catch (Exception e) {
                        logger.error("Pipeline {} : unexpected error stopping engine {}: {}", pipeline.getName(), engineContext.getName(), e.getMessage());
                    }
                }));
        //add new items
        pipelines.stream()
                .filter(pipeline -> !registry.containsKey(pipeline))
                .collect(Collectors.toList())
                .forEach(pipeline -> {

                });

    }
}
