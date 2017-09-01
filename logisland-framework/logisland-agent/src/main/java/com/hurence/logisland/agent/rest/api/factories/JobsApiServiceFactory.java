package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.JobsApiService;
import com.hurence.logisland.agent.rest.api.impl.JobsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class JobsApiServiceFactory {
    private static JobsApiService service = null;

    public static JobsApiService getJobsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new JobsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}