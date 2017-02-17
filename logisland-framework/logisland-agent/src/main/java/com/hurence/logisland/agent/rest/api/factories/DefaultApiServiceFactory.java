package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.DefaultApiService;
import com.hurence.logisland.agent.rest.api.impl.DefaultApiServiceImpl;

import com.hurence.logisland.kakfa.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:12:48.738+01:00")
public class DefaultApiServiceFactory {
    private static DefaultApiService service = null;

    public static DefaultApiService getDefaultApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new DefaultApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}