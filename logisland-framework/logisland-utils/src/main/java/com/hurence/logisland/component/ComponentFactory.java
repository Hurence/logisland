/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.component;

import com.hurence.logisland.config.ControllerServiceConfiguration;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
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
            final ProcessingEngine engine =
                    (ProcessingEngine) Class.forName(configuration.getComponent()).newInstance();
            final EngineContext engineContext =
                    new StandardEngineContext(engine, Long.toString(currentId.incrementAndGet()));


            // instanciate each related pipelineContext
            configuration.getStreamConfigurations().forEach(pipelineConfig -> {
                Optional<StreamContext> pipelineContext = getStreamContext(pipelineConfig);
                if (pipelineContext.isPresent())
                    engineContext.addStreamContext(pipelineContext.get());
            });

            configuration.getConfiguration()
                    .entrySet().forEach(e -> engineContext.setProperty(e.getKey(), e.getValue()));


            // load all controller service initialization context
            configuration.getControllerServiceConfigurations().forEach(serviceConfig -> {
                engineContext.addControllerServiceConfiguration(serviceConfig);
            });


            logger.info("created engine {}", configuration.getComponent());


            return Optional.of(engineContext);

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
    public static Optional<StreamContext> getStreamContext(StreamConfiguration configuration) {
        try {
            final RecordStream recordStream =
                    (RecordStream) Class.forName(configuration.getComponent()).newInstance();
            final StreamContext instance =
                    new StandardStreamContext(recordStream, Long.toString(currentId.incrementAndGet()));

            // instanciate each related processor
            configuration.getProcessorConfigurations().forEach(processConfig -> {
                Optional<ProcessContext> processorContext = getProcessContext(processConfig);
                if (processorContext.isPresent())
                    instance.addProcessContext(processorContext.get());
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

    public static Optional<ProcessContext> getProcessContext(ProcessorConfiguration configuration) {
        try {
            final Processor processor = (Processor) Class.forName(configuration.getComponent()).newInstance();
            final ProcessContext processContext =
                    new StandardProcessContext(processor, Long.toString(currentId.incrementAndGet()));

            // set all properties
            configuration.getConfiguration()
                    .entrySet().forEach(e -> processContext.setProperty(e.getKey(), e.getValue()));

            logger.info("created processor {}", configuration.getComponent());
            return Optional.of(processContext);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.error("unable to instanciate processor {} : {}", configuration.getComponent(), e.toString());
        }

        return Optional.empty();
    }


}
