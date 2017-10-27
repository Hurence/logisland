// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.agent.rest.api.MetricsApiService;
import com.hurence.logisland.agent.rest.api.factories.MetricsApiServiceFactory;

import io.swagger.annotations.ApiParam;

    import javax.validation.constraints.*;

import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@Path("/metrics")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the metrics API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class MetricsApi {

    private final MetricsApiService delegate;

    public MetricsApi(KafkaRegistry kafkaRegistry) {
        this.delegate = MetricsApiServiceFactory.getMetricsApi(kafkaRegistry);
    }

    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "text/plain" })
    @io.swagger.annotations.ApiOperation(value = "retrieve all job metrics in Prometheus format", notes = "get Prometheus metrics. have a look to https://prometheus.io/docs/instrumenting/exposition_formats/", response = String.class, tags={ "metrics" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "metrics", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getMetrics(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getMetrics(securityContext);
    }
    }
