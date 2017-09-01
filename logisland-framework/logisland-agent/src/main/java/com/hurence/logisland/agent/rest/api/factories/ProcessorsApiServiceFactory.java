package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.ProcessorsApiService;
import com.hurence.logisland.agent.rest.api.impl.ProcessorsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class ProcessorsApiServiceFactory {
    private static ProcessorsApiService service = null;

    public static ProcessorsApiService getProcessorsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new ProcessorsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}