package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.ConfigsApiService;
import com.hurence.logisland.agent.rest.api.impl.ConfigsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-28T17:12:21.474+01:00")
public class ConfigsApiServiceFactory {
    private static ConfigsApiService service = null;

    public static ConfigsApiService getConfigsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ConfigsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}