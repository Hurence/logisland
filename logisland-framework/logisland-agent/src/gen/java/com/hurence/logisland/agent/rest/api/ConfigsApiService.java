package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;



import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Property;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import com.hurence.logisland.kafka.registry.KafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
    import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public abstract class ConfigsApiService {

    protected final KafkaRegistry kafkaRegistry;

    public ConfigsApiService(KafkaRegistry kafkaRegistry) {
        this.kafkaRegistry = kafkaRegistry;
    }
        public abstract Response getConfig(SecurityContext securityContext)
        throws NotFoundException;
    }
