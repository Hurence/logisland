package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public abstract class ProcessorsApiService {

    protected final KafkaRegistry kafkaRegistry;

    public ProcessorsApiService(KafkaRegistry kafkaRegistry) {
        this.kafkaRegistry = kafkaRegistry;
    }

    public abstract Response getProcessors(SecurityContext securityContext)
            throws NotFoundException;
}
