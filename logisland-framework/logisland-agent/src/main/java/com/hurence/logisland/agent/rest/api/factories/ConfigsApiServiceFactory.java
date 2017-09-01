package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.ConfigsApiService;
import com.hurence.logisland.agent.rest.api.impl.ConfigsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class ConfigsApiServiceFactory {
    private static ConfigsApiService service = null;

    public static ConfigsApiService getConfigsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ConfigsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}