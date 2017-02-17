package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.TopicsApiService;
import com.hurence.logisland.agent.rest.api.impl.TopicsApiServiceImpl;

import com.hurence.logisland.kakfa.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:12:48.738+01:00")
public class TopicsApiServiceFactory {
    private static TopicsApiService service = null;

    public static TopicsApiService getTopicsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new TopicsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}