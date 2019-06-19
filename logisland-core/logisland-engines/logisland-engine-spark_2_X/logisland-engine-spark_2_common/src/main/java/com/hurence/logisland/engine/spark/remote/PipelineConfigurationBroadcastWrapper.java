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

import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.stream.StreamContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link Broadcast} wrapper for a Stream pipeline configuration.
 * This class allow to magically synchronize data modified from the spark driver to every executor.
 *
 * @author amarziali
 */
public class PipelineConfigurationBroadcastWrapper {
    private static final Logger logger = LoggerFactory.getLogger(PipelineConfigurationBroadcastWrapper.class);

    private Broadcast<Map<String, Collection<ProcessContext>>> broadcastedPipelineMap;

    private static PipelineConfigurationBroadcastWrapper obj = new PipelineConfigurationBroadcastWrapper();

    private PipelineConfigurationBroadcastWrapper() {
    }

    public static PipelineConfigurationBroadcastWrapper getInstance() {
        return obj;
    }

    public JavaSparkContext getSparkContext(SparkContext sc) {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        return jsc;
    }

    public void refresh(Map<String, Collection<ProcessContext>> pipelineMap, SparkContext sparkContext) {
        logger.info("Refreshing dataflow pipelines!");

        if (broadcastedPipelineMap != null) {
            broadcastedPipelineMap.unpersist();
        }
        broadcastedPipelineMap = getSparkContext(sparkContext).broadcast(pipelineMap);
    }

    public void refresh(EngineContext engineContext, SparkContext sparkContext) {
        logger.info("Refreshing dataflow pipelines!");
        Map<String, Collection<ProcessContext>> pipelineMap = engineContext.getStreamContexts().stream()
                .collect(
                        Collectors.toMap(
                                StreamContext::getIdentifier,
                                StreamContext::getProcessContexts
                        )
                );
        refresh(pipelineMap, sparkContext);
    }


    public Collection<ProcessContext> get(String streamName) {
        return broadcastedPipelineMap.getValue().get(streamName);
    }
}