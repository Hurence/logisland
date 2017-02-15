package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;



import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Job;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import com.hurence.logisland.kakfa.registry.LogislandKafkaRegistry;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T12:36:09.930+01:00")
public abstract class JobsApiService {

    protected final LogislandKafkaRegistry kafkaRegistry;

    public JobsApiService(LogislandKafkaRegistry kafkaRegistry) {
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
        public abstract Response getJobErrors(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobMetrics(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response getJobStatus(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response pauseJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response shutdownJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response startJob(String jobId,SecurityContext securityContext)
        throws NotFoundException;
        public abstract Response updateJob(String jobId,Job job,SecurityContext securityContext)
        throws NotFoundException;
    }
