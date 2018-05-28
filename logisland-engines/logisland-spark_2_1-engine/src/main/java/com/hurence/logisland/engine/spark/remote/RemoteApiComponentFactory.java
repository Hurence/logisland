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
import com.hurence.logisland.engine.spark.remote.model.Processor;
import com.hurence.logisland.engine.spark.remote.model.Property;
import com.hurence.logisland.engine.spark.remote.model.Service;
import com.hurence.logisland.engine.spark.remote.model.Stream;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.stream.RecordStream;
import com.hurence.logisland.stream.StandardStreamContext;
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.stream.Collectors;

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
            stream.getProcessors().forEach(processor -> {
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
}
