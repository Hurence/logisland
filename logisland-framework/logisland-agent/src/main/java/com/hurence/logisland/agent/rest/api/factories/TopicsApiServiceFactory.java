package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.TopicsApiService;
import com.hurence.logisland.agent.rest.api.impl.TopicsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class TopicsApiServiceFactory {
    private static TopicsApiService service = null;

    public static TopicsApiService getTopicsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new TopicsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}