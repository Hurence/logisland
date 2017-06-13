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
// hola
package com.hurence.logisland.agent.rest.api;

import com.hurence.logisland.agent.rest.model.*;
import com.hurence.logisland.agent.rest.api.JobsApiService;
import com.hurence.logisland.agent.rest.api.factories.JobsApiServiceFactory;

import io.swagger.annotations.ApiParam;


import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Job;
import com.hurence.logisland.agent.rest.model.Metrics;

import java.util.List;
import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;


import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

import com.hurence.logisland.kafka.registry.KafkaRegistry;

@Path("/jobs")
@Consumes({ "application/json" })
@Produces({ "application/json" })
@io.swagger.annotations.Api(description = "the jobs API")
@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-03-23T11:55:20.570+01:00")
public class JobsApi {

    private final JobsApiService delegate;

    public JobsApi(KafkaRegistry kafkaRegistry) {
        this.delegate = JobsApiServiceFactory.getJobsApi(kafkaRegistry);
    }

    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "create new job", notes = "store a new job configuration if valid", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Job successfuly created", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 400, message = "Invalid ID supplied", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 404, message = "Job not found", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response addJob(
    @ApiParam(value = "Job to add to the store" ,required=true) Job job
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.addJob(job,securityContext);
    }
    @POST
    @Path("/{jobId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "create new job", notes = "store a new job configuration if valid", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Job successfuly created", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 400, message = "Invalid ID supplied", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 404, message = "Job not found", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response addJobWithId(
    @ApiParam(value = "Job configuration to add to the store" ,required=true) Job body
,
    @ApiParam(value = "JobId to add to the store",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.addJobWithId(body,jobId,securityContext);
    }
    @DELETE
    @Path("/{jobId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "delete job", notes = "remove the corresponding Job definition and stop if its currently running", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfully removed", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 400, message = "Invalid ID supplied", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 404, message = "Job not found", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response deleteJob(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.deleteJob(jobId,securityContext);
    }
    @GET
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get all jobs", notes = "retrieve all jobs (retrieve only summary fields)", response = Job.class, responseContainer = "List", tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job configuration list", response = Job.class, responseContainer = "List"),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class, responseContainer = "List") })
    public Response getAllJobs(
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getAllJobs(securityContext);
    }
    @GET
    @Path("/{jobId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json", "text/plain" })
    @io.swagger.annotations.ApiOperation(value = "get job", notes = "get the corresponding Job definition", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job definition", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response getJob(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJob(jobId,securityContext);
    }
    @GET
    @Path("/alerts")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get job alerts", notes = "get the alerts", response = Metrics.class, responseContainer = "List", tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job metrics", response = Metrics.class, responseContainer = "List"),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Metrics.class, responseContainer = "List") })
    public Response getJobAlerts(
    @ApiParam(value = "max number of ites to retrieve", defaultValue="20") @DefaultValue("20") @QueryParam("count") Integer count
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobAlerts(count,securityContext);
    }
    @GET
    @Path("/{jobId}/engine")
    @Consumes({ "application/json" })
    @Produces({ "text/plain" })
    @io.swagger.annotations.ApiOperation(value = "get job engine configuration", notes = "this is usefull when you want to launch a spark app within YARN to retrieve the launching config before submitting the job itself", response = String.class, tags={ "job", "engine",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job status", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getJobEngine(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobEngine(jobId,securityContext);
    }
    @GET
    @Path("/errors")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get last job errors", notes = "get the errors", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job errors", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response getJobErrors(
    @ApiParam(value = "max number of ites to retrieve", defaultValue="20") @DefaultValue("20") @QueryParam("count") Integer count
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobErrors(count,securityContext);
    }
    @GET
    @Path("/metrics")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get job metrics", notes = "get the metrics of corresponding Job", response = Metrics.class, responseContainer = "List", tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job metrics", response = Metrics.class, responseContainer = "List"),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Metrics.class, responseContainer = "List") })
    public Response getJobMetrics(
    @ApiParam(value = "max number of ites to retrieve", defaultValue="20") @DefaultValue("20") @QueryParam("count") Integer count
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobMetrics(count,securityContext);
    }
    @GET
    @Path("/{jobId}/status")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get job status", notes = "get the status of corresponding Job", response = String.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job status", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getJobStatus(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobStatus(jobId,securityContext);
    }
    @GET
    @Path("/{jobId}/version")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get job version", notes = "get the version of corresponding Job", response = String.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job version", response = String.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = String.class) })
    public Response getJobVersion(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.getJobVersion(jobId,securityContext);
    }
    @POST
    @Path("/{jobId}/pause")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "pause job", notes = "pause the corresponding Job", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfuly paused", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response pauseJob(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.pauseJob(jobId,securityContext);
    }
    @POST
    @Path("/{jobId}/restart")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "start job", notes = "start the corresponding Job definition", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfuly started", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response reStartJob(
    @ApiParam(value = "id of the job to restart",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.reStartJob(jobId,securityContext);
    }
    @POST
    @Path("/{jobId}/shutdown")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "shutdown job", notes = "shutdown the running Job", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfuly started", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response shutdownJob(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.shutdownJob(jobId,securityContext);
    }
    @POST
    @Path("/{jobId}/start")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "start job", notes = "start the corresponding Job definition", response = Job.class, tags={ "job",  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "job successfuly started", response = Job.class),
        @io.swagger.annotations.ApiResponse(code = 200, message = "unexpected error", response = Job.class) })
    public Response startJob(
    @ApiParam(value = "id of the job to return",required=true) @PathParam("jobId") String jobId
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.startJob(jobId,securityContext);
    }
    @PUT
    @Path("/{jobId}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "update job", notes = "update an existing job configuration if valid", response = Job.class, tags={ "job" })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Job successfuly created", response = Job.class) })
    public Response updateJob(
    @ApiParam(value = "Job to add to the store",required=true) @PathParam("jobId") String jobId
,
    @ApiParam(value = "Job to add to the store" ,required=true) Job job
,
    @Context SecurityContext securityContext)
    throws NotFoundException {
    return delegate.updateJob(jobId,job,securityContext);
    }
    }
