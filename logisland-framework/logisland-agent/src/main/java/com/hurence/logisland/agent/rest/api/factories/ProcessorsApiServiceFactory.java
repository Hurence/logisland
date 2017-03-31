package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.ProcessorsApiService;
import com.hurence.logisland.agent.rest.api.impl.ProcessorsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class ProcessorsApiServiceFactory {
    private static ProcessorsApiService service = null;

    public static ProcessorsApiService getProcessorsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ProcessorsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}