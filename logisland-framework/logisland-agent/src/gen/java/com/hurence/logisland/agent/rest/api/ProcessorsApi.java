// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.agent.rest.api.ProcessorsApiService;
import com.hurence.logisland.agent.rest.api.factories.ProcessorsApiServiceFactory;

import io.swagger.annotations.ApiParam;


import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@Path("/processors")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the processors API")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class ProcessorsApi {

    private final ProcessorsApiService delegate;

    public ProcessorsApi(KafkaRegistry kafkaRegistry) {
        this.delegate = ProcessorsApiServiceFactory.getProcessorsApi(kafkaRegistry);
    }

    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get all processors", notes = "get all processors", response = String.class, tags={ "config" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "processors", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getProcessors(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getProcessors(securityContext);
    }
    }
