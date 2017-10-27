package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.DefaultApiService;
import com.hurence.logisland.agent.rest.api.impl.DefaultApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class DefaultApiServiceFactory {
    private static DefaultApiService service = null;

    public static DefaultApiService getDefaultApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new DefaultApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}