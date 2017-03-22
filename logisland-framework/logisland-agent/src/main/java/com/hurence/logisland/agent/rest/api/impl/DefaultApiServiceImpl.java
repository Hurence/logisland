package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;


import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-17T11:14:18.946+01:00")
    public class DefaultApiServiceImpl extends DefaultApiService {

    public DefaultApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

        @Override
        public Response rootGet(SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
        }
    }
