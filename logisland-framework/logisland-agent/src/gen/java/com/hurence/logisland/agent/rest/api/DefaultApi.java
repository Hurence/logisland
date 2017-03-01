// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.agent.rest.api.DefaultApiService;
import com.hurence.logisland.agent.rest.api.factories.DefaultApiServiceFactory;

import io.swagger.annotations.ApiParam;



import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kakfa.registry.KafkaRegistry;

@Path("/")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the  API")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-01T22:13:54.411+01:00")
public class DefaultApi {

    private final DefaultApiService delegate;

    public DefaultApi(KafkaRegistry kafkaRegistry) {
        this.delegate = DefaultApiServiceFactory.getDefaultApi(kafkaRegistry);
    }

    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "the root resource", notes = "/ entrypoint", response = void.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "OK", response = void.class) })
    public Response rootGet(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.rootGet(securityContext);
    }
    }
