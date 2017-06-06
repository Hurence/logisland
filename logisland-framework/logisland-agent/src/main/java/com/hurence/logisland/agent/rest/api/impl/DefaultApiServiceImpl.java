/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.*;


import com.hurence.logisland.agent.rest.api.NotFoundException;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

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
