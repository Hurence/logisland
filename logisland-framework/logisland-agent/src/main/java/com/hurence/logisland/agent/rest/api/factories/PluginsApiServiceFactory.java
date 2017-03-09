package com.hurence.logisland.agent.rest.api.factories;

import com.hurence.logisland.agent.rest.api.PluginsApiService;
import com.hurence.logisland.agent.rest.api.impl.PluginsApiServiceImpl;

import com.hurence.logisland.kakfa.registry.KafkaRegistry;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-03T16:47:02.913+01:00")
public class PluginsApiServiceFactory {
    private static PluginsApiService service = null;

    public static PluginsApiService getPluginsApi(KafkaRegistry kafkaRegistry) {
        if (service == null) {
            service = new PluginsApiServiceImpl(kafkaRegistry);
        }
        return service;
    }
}