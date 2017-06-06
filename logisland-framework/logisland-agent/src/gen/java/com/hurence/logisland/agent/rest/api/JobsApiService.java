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
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;



import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Job;
import com.hurence.logisland.agent.rest.model.Metrics;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import com.hurence.logisland.kafka.registry.KafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public abstract class JobsApiService {

    protected final KafkaRegistry kafkaRegistry;

    public JobsApiService(KafkaRegistry kafkaRegistry) {
        this.kafkaRegistry = kafkaRegistry;
    }
        public abstract Response addJob(Job job,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response addJobWithId(Job body,String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response deleteJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getAllJobs(SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobAlerts(Integer count,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobEngine(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobErrors(Integer count,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobMetrics(Integer count,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobStatus(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobVersion(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response pauseJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response reStartJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response shutdownJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response startJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response updateJob(String jobId,Job job,SecurityContext securityContext)
        throws NotFoundException;
    }
