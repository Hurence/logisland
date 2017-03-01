package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.ApiResponseMessage;
import com.hurence.logisland.agent.rest.api.NotFoundException;
import com.hurence.logisland.agent.rest.api.TopicsApiService;
import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Topic;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.Date;
import java.util.List;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:14:18.946+01:00")
public class TopicsApiServiceImpl extends TopicsApiService {

    public TopicsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

    private static Logger logger = LoggerFactory.getLogger(TopicsApiServiceImpl.class);

    @Override
    public Response addNewTopic(Topic body, SecurityContext securityContext) throws NotFoundException {


        logger.debug("adding topic " + body);

        try {
            Topic newTopic = kafkaRegistry.addTopic(body);
            return Response.ok().entity(newTopic).build();
        } catch (RegistryException e) {
            String error = "unable to add topic into kafkastore " + e;
            logger.error(error);
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, error))
                    .build();
        }
    }

    @Override
    public Response deleteTopic(String topicId, SecurityContext securityContext) throws NotFoundException {
        logger.debug("delete topic");
        try {
            kafkaRegistry.deleteTopic(topicId);
            //TODO delete the topic in Kafka

            return Response.ok().build();
        } catch (RegistryException e) {
            String error = "unable to delete topic" + topicId + " from kafkastore " + e;
            logger.error(error);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, error))
                    .build();
        }
    }

    @Override
    public Response getAllTopics(SecurityContext securityContext) throws NotFoundException {
        logger.debug("get all topics");
        try {
            List<Topic> topics = kafkaRegistry.getAllTopics();
            return Response.ok().entity(topics).build();
        } catch (RegistryException e) {
            String error = "unable to get alls topics from kafkastore " + e;
            logger.error(error);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, error))
                    .build();
        }
    }

    @Override
    public Response getTopic(String topicId, SecurityContext securityContext) throws NotFoundException {
        logger.debug("get topic " + topicId);


        Topic topic = null;
        try {
            topic = kafkaRegistry.getTopic(topicId);
        } catch (RegistryException e) {
            return Response.serverError().entity(e).build();
        }

        if (topic == null)
            return Response.serverError()
                    .status(Response.Status.NOT_FOUND)
                    .entity(new Error().code(404).message("Topic not found for id: " + topicId))
                    .build();
        else
            return Response.ok().entity(topic).build();
    }

    @Override
    public Response updateTopic(Topic body, String topicId, SecurityContext securityContext) throws NotFoundException {
        logger.debug("update topic " + body);

        try {
            Topic topic = kafkaRegistry.updateTopic(body);
            return Response.ok().entity(topic).build();
        } catch (RegistryException e) {
            String error = "unable to update topic into kafkastore " + e;
            logger.error(error);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, error))
                    .build();
        }
    }

    @Override
    public Response checkTopicKeySchemaCompatibility(String body, String topicId, SecurityContext securityContext) throws NotFoundException {
        return Response.status(Response.Status.NOT_IMPLEMENTED)
                .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, "not implemented yet"))
                .build();
    }

    @Override
    public Response checkTopicValueSchemaCompatibility(String topicId, String body, SecurityContext securityContext) throws NotFoundException {
        return Response.status(Response.Status.NOT_IMPLEMENTED)
                .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, "not implemented yet"))
                .build();
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
