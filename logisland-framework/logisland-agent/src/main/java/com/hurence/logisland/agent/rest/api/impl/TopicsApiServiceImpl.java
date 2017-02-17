package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;

import com.hurence.logisland.agent.rest.model.Topic;
import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:14:18.946+01:00")
    public class TopicsApiServiceImpl extends TopicsApiService {

    public TopicsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

        @Override
        public Response addNewTopic(Topic body, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response checkTopicKeySchemaCompatibility(String body, String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response checkTopicValueSchemaCompatibility(String topicId, String body, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response deleteTopic(String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response getAllTopics(SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response getTopic(String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response getTopicKeySchema(String topicId, String version, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response getTopicValueSchema(String topicId, String version, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response updateTopic(Topic body, String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response updateTopicKeySchema(String body, String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
        @Override
        public Response updateTopicValueSchema(String body, String topicId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
    }
