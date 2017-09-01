package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.MetricsApiService;
import com.hurence.logisland.agent.rest.api.impl.MetricsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class MetricsApiServiceFactory {
    private static MetricsApiService service = null;

    public static MetricsApiService getMetricsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new MetricsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}