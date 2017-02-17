// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.agent.rest.api.TopicsApiService;
import com.hurence.logisland.agent.rest.api.factories.TopicsApiServiceFactory;

import io.swagger.annotations.ApiParam;


import com.hurence.logisland.agent.rest.model.Topic;
import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kakfa.registry.KafkaRegistry;

@Path("/topics")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the topics API")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:15:05.350+01:00")
public class TopicsApi {

    private final TopicsApiService delegate;

    public TopicsApi(KafkaRegistry kafkaRegistry) {
        this.delegate = TopicsApiServiceFactory.getTopicsApi(kafkaRegistry);
    }

    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "create new topic", notes = "", response = void.class, tags={ "topic",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Status 200", response = void.class) })
    public Response addNewTopic(
    @ApiParam(value = "" ,required=true) Topic body
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.addNewTopic(body,securityContext);
    }
    @POST
    @Path("/{topicId}/keySchema/checkCompatibility")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "check topic key schema compatibility", notes = "", response = String.class, tags={ "schema",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "compatibility level", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response checkTopicKeySchemaCompatibility(
    @ApiParam(value = "Avro schema as a json string" ,required=true) String body
,
    @ApiParam(value = "id of the job to return",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.checkTopicKeySchemaCompatibility(body,topicId,securityContext);
    }
    @POST
    @Path("/{topicId}/valueSchema/checkCompatibility")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "check topic value schema compatibility", notes = "", response = String.class, tags={ "schema",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "compatibility level", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response checkTopicValueSchemaCompatibility(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("topicId") String topicId
,
    @ApiParam(value = "Avro schema as a json string" ,required=true) String body
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.checkTopicValueSchemaCompatibility(topicId,body,securityContext);
    }
    @DELETE
    @Path("/{topicId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "delete topic", notes = "remove a topic config and remove all content from Kafka", response = String.class, tags={ "topic",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "topic successfully deleted", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response deleteTopic(
    @ApiParam(value = "",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.deleteTopic(topicId,securityContext);
    }
    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get all topics", notes = "", response = Topic.class, responseContainer = "List", tags={ "topic",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Status 200", response = Topic.class, responseContainer = "List") })
    public Response getAllTopics(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getAllTopics(securityContext);
    }
    @GET
    @Path("/{topicId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get topic", notes = "", response = Topic.class, tags={ "topic",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Status 200", response = Topic.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Topic.class) })
    public Response getTopic(
    @ApiParam(value = "",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getTopic(topicId,securityContext);
    }
    @GET
    @Path("/{topicId}/keySchema")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get topic key schema", notes = "", response = String.class, tags={ "schema",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Avro schema", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getTopicKeySchema(
    @ApiParam(value = "",required=true) @PathParam("topicId") String topicId
,
    @ApiParam(value = "version of the schema (\"latest\" if not provided)", defaultValue="latest") @DefaultValue("latest") @QueryParam("version") String version
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getTopicKeySchema(topicId,version,securityContext);
    }
    @GET
    @Path("/{topicId}/valueSchema")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get topic value schema", notes = "", response = String.class, tags={ "schema",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job definition", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getTopicValueSchema(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("topicId") String topicId
,
    @ApiParam(value = "version of the schema (\"latest\" if not provided)", defaultValue="latest") @DefaultValue("latest") @QueryParam("version") String version
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getTopicValueSchema(topicId,version,securityContext);
    }
    @PUT
    @Path("/{topicId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "update topic", notes = "", response = Topic.class, tags={ "topic",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfuly started", response = Topic.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Topic.class) })
    public Response updateTopic(
    @ApiParam(value = "" ,required=true) Topic body
,
    @ApiParam(value = "",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.updateTopic(body,topicId,securityContext);
    }
    @PUT
    @Path("/{topicId}/keySchema")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "update topic key schema", notes = "", response = String.class, tags={ "schema",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Avro schema", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response updateTopicKeySchema(
    @ApiParam(value = "schema to add to the store" ,required=true) String body
,
    @ApiParam(value = "id of the job to return",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.updateTopicKeySchema(body,topicId,securityContext);
    }
    @PUT
    @Path("/{topicId}/valueSchema")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "update topic value schema", notes = "", response = String.class, tags={ "schema" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Avro schema", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response updateTopicValueSchema(
    @ApiParam(value = "Avro schema as a json string" ,required=true) String body
,
    @ApiParam(value = "id of the job to return",required=true) @PathParam("topicId") String topicId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.updateTopicValueSchema(body,topicId,securityContext);
    }
    }
