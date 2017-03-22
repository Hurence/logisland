// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.api.factories.ConfigsApiServiceFactory;


import com.hurence.logisland.agent.rest.model.Property;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@Path("/configs")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the configs API")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-03T16:47:02.913+01:00")
public class ConfigsApi {

    private final ConfigsApiService delegate;

    public ConfigsApi(KafkaRegistry kafkaRegistry) {
        this.delegate = ConfigsApiServiceFactory.getConfigsApi(kafkaRegistry);
    }

    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "global config", notes = "get all global configuration properties", response = Property.class, responseContainer = "List", tags={ "config" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "global configuration", response = Property.class, responseContainer = "List"),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Property.class, responseContainer = "List") })
    public Response getConfig(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getConfig(securityContext);
    }
    }
