/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.hurence.logisland.config;

import com.hurence.logisland.engine.StandardEngineInstance;
import com.hurence.logisland.chain.StandardProcessorChainInstance;
import com.hurence.logisland.engine.StreamProcessingEngine;
import com.hurence.logisland.processor.AbstractProcessor;
import com.hurence.logisland.processor.Processor;
import com.hurence.logisland.processor.StandardProcessorInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


public final class ComponentFactory {

    private static Logger logger = LoggerFactory.getLogger(ComponentFactory.class);

    private static final AtomicLong currentId = new AtomicLong(0);


    public static Optional<StandardEngineInstance> getEngineInstance(EngineConfiguration configuration) {
        try {
            final StreamProcessingEngine processor =
                    (StreamProcessingEngine) Class.forName(configuration.getComponent()).newInstance();
            final StandardEngineInstance engineInstance =
                    new StandardEngineInstance(processor, Long.toString(currentId.incrementAndGet()));


            // instanciate each related processorChainInstance
            configuration.getProcessorChains().forEach(processChainConfig -> {
                Optional<StandardProcessorChainInstance> processorChainInstance = getProcessorChainInstance(processChainConfig);
                if (processorChainInstance.isPresent())
                    engineInstance.addProcessorChainInstance(processorChainInstance.get());
            });

            configuration.getConfiguration()
                    .entrySet().forEach(e -> engineInstance.setProperty(e.getKey(), e.getValue()));

            logger.info("created engine {}", configuration.getComponent());

            return Optional.of(engineInstance);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instanciate engine {} : {}", configuration.getComponent(), e.toString());
        }
        return Optional.empty();
    }

    /**
     * Instanciates a stream from of configuration
     *
     * @param configuration
     * @return
     */
    public static Optional<StandardProcessorChainInstance> getProcessorChainInstance(ProcessorChainConfiguration configuration) {
        try {
            final AbstractProcessor processorChain =
                    (AbstractProcessor) Class.forName(configuration.getComponent()).newInstance();
            final StandardProcessorChainInstance instance =
                    new StandardProcessorChainInstance(processorChain, Long.toString(currentId.incrementAndGet()));

            // instanciate each related processor
            configuration.getProcessors().forEach(processConfig -> {
                Optional<StandardProcessorInstance> processorInstance = getProcessorInstance(processConfig);
                if (processorInstance.isPresent())
                    instance.addProcessorInstance(processorInstance.get());
            });

            // set the config properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> instance.setProperty(e.getKey(), e.getValue()));
            logger.info("created processor {}", configuration.getComponent());
            return Optional.of(instance);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instanciate processor {} : {}", configuration.getComponent(), e.toString());
        }
        return Optional.empty();
    }

    public static Optional<StandardProcessorInstance> getProcessorInstance(ProcessorConfiguration configuration) {
        try {
            final Processor processor = (Processor) Class.forName(configuration.getComponent()).newInstance();
            final StandardProcessorInstance instance =
                    new StandardProcessorInstance(processor, Long.toString(currentId.incrementAndGet()));

            // set all properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> instance.setProperty(e.getKey(), e.getValue()));

            logger.info("created processor {}", configuration.getComponent());
            return Optional.of(instance);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instanciate processor {} : {}", configuration.getComponent(), e.toString());
        }

        return Optional.empty();
    }


}
