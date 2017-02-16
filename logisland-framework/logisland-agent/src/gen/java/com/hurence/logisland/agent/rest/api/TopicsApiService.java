package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;



import com.hurence.logisland.agent.rest.model.Topic;
import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-16T15:07:25.192+01:00")
public abstract class TopicsApiService {

    protected final KafkaRegistry kafkaRegistry;

    public TopicsApiService(KafkaRegistry kafkaRegistry) {
        this.kafkaRegistry = kafkaRegistry;
    }
        public abstract Response addNewTopic(Topic body,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response checkTopicKeySchemaCompatibility(String body,String topicId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response checkTopicValueSchemaCompatibility(String topicId,String body,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response deleteTopic(String topicId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getAllTopics(SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getTopic(String topicId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getTopicKeySchema(String topicId,String version,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getTopicValueSchema(String topicId,String version,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response updateTopic(Topic body,String topicId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response updateTopicKeySchema(String body,String topicId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response updateTopicValueSchema(String body,String topicId,SecurityContext securityContext)
        throws NotFoundException;
    }
