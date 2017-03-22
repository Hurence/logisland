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

import com.hurence.logisland.agent.rest.client.*;
import com.hurence.logisland.agent.rest.client.exceptions.RestClientException;
import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.ProcessingEngine;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.StandardProcessContext;
import com.hurence.logisland.stream.RecordStream;
import com.hurence.logisland.stream.StandardStreamContext;
import com.hurence.logisland.stream.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;


public final class RestComponentFactory {

    private final JobsApiClient restJobsApiClient;
    private final TopicsApiClient topicsApiClient;
    private final ConfigsApiClient configsApiClient;
    private final String agentQuorum;


    public RestComponentFactory(String agentQuorum) {
        this.restJobsApiClient = new RestJobsApiClient(agentQuorum);
        this.topicsApiClient = new RestTopicsApiClient(agentQuorum);
        this.configsApiClient = new RestConfigsApiClient(agentQuorum);
        this.agentQuorum = agentQuorum;
    }

    public RestComponentFactory(JobsApiClient jobsApiClient, TopicsApiClient topicsApiClient, ConfigsApiClient configsApiClient) {
        this.restJobsApiClient = jobsApiClient;
        this.topicsApiClient = topicsApiClient;
        this.configsApiClient = configsApiClient;
        this.agentQuorum = "http://localhost:8081";
    }

    private Logger logger = LoggerFactory.getLogger(RestComponentFactory.class);


    public Optional<EngineContext> getEngineContext(String jobName) {
        try {

            // get job from api
            Job job = restJobsApiClient.getJob(jobName);
            if (job != null) {


                final ProcessingEngine engine =
                        (ProcessingEngine) Class.forName(job.getEngine().getComponent()).newInstance();
                final EngineContext engineContext =
                        new StandardEngineContext(engine, job.getId().toString());


                // instanciate each related processorChainContext
                job.getStreams().forEach(stream -> {
                    Optional<StreamContext> processorChainContext = getStreamContext(stream);
                    if (processorChainContext.isPresent())
                        engineContext.addStreamContext(processorChainContext.get());
                });

                job.getEngine().getConfig().forEach(e -> engineContext.setProperty(e.getKey(), e.getValue()));

                engineContext.setName(jobName);
                logger.info("created engine {}", job.getEngine());

                return Optional.of(engineContext);
            } else
                return Optional.empty();


        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RestClientException e) {
            logger.error("unable to instanciate job {} : {}", jobName, e);
        }
        return Optional.empty();
    }

    /**
     * Instanciates a stream from of configuration
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

            // instanciate each related processor
            stream.getProcessors().forEach(processor -> {
                Optional<ProcessContext> processorContext = getProcessContext(processor);
                if (processorContext.isPresent())
                    instance.addProcessContext(processorContext.get());
            });

            // set the config properties
            stream.getConfig().forEach(e -> {

                String key = e.getKey();
                String value = e.getValue();
                switch (key) {
                    case "kafka.input.topics": {
                        Topic topic = null;
                        try {
                            topic = topicsApiClient.getTopic(value);
                            if (topic == null) {
                                logger.error("{} topic was not found", value);
                            } else {
                                instance.setProperty("kafka.input.topics.serializer", topic.getSerializer());
                                instance.setProperty(key, value);
                            }
                        } catch (RestClientException e1) {
                            logger.error("{} topic was not found", e1.toString());
                        }

                        break;
                    }
                    case "kafka.output.topics": {
                        try {
                            Topic topic = topicsApiClient.getTopic(value);
                            if (topic == null) {
                                logger.error("{} topic was not found", value);
                            } else {
                                instance.setProperty("kafka.output.topics.serializer", topic.getSerializer());
                                instance.setProperty(key, value);
                            }
                        } catch (RestClientException e1) {
                            logger.error("{} topic was not found", e1.toString());
                        }
                        break;
                    }
                }
                try {
                    List<Property> configs = configsApiClient.getConfigs();
                    configs.forEach(conf -> {
                        instance.setProperty(conf.getKey(), conf.getValue());
                    });
                } catch (RestClientException e1) {
                    logger.error("{} topic was not found", e1.toString());
                }

                instance.setProperty("logisland.agent.quorum", this.agentQuorum);

            });
            logger.info("created stream {}", stream);
            return Optional.of(instance);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException  e) {
            logger.error("unable to instanciate stream {} : {}", stream, e.toString());
        }
        return Optional.empty();
    }

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
            logger.error("unable to instanciate processor {} : {}", processor.getComponent(), e.toString());
        }

        return Optional.empty();
    }


    public static void main(String[] args) {
        RestComponentFactory factory = new RestComponentFactory("http://localhost:8081");

        factory.getEngineContext("IndexApacheLogsDemo");
    }

}
