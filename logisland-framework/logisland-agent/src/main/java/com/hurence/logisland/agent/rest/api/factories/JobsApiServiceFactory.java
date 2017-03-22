package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.JobsApiService;
import com.hurence.logisland.agent.rest.api.impl.JobsApiServiceImpl;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:12:48.738+01:00")
public class JobsApiServiceFactory {
    private static JobsApiService service = null;

    public static JobsApiService getJobsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new JobsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}