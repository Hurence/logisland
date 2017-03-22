package com.hurence.logisland.agent.rest.api;


import com.hurence.logisland.kafka.registry.KafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-03T16:47:02.913+01:00")
public abstract class PluginsApiService {

    protected final KafkaRegistry kafkaRegistry;

    public PluginsApiService(KafkaRegistry kafkaRegistry) {
        this.kafkaRegistry = kafkaRegistry;
    }
        public abstract Response getPlugins(SecurityContext securityContext)
        throws NotFoundException;
    }
