package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.ApiResponseMessage;
import com.hurence.logisland.agent.rest.api.JobsApiService;
import com.hurence.logisland.agent.rest.api.NotFoundException;
import com.hurence.logisland.agent.rest.model.Error;
import com.hurence.logisland.agent.rest.model.Job;
import com.hurence.logisland.kakfa.registry.KafkaRegistry;
import com.hurence.logisland.kakfa.registry.exceptions.RegistryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.List;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T10:15:35.873+01:00")
public class JobsApiServiceImpl extends JobsApiService {

    public JobsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }


    private static Logger logger = LoggerFactory.getLogger(JobsApiService.class);


    //------------------------
    //  CRUD Jobs section
    //------------------------

    @Override
    public Response addJobWithId(Job body, String jobId, SecurityContext securityContext) throws NotFoundException {
        return addJob(body.name(jobId), securityContext);
    }

    @Override
    public Response addJob(Job job, SecurityContext securityContext) throws NotFoundException {
        logger.debug("adding job " + job);

        try {
            Job job0 = kafkaRegistry.addJob(job);
            return Response.ok().entity(job0).build();
        } catch (RegistryException e) {
            String error = "unable to add job into kafkastore " + e;
            logger.error(error);
            return Response.serverError().entity(error).build();
        }


    }

    @Override
    public Response updateJob(String jobId, Job job, SecurityContext securityContext) throws NotFoundException {
        logger.debug("update job " + job);

        try {
            Job job0 = kafkaRegistry.updateJob(job);
            return Response.ok().entity(job0).build();
        } catch (RegistryException e) {
            String error = "unable to update job into kafkastore " + e;
            logger.error(error);
            return Response.serverError().entity(error).build();
        }
    }


    @Override
    public Response deleteJob(String jobId, SecurityContext securityContext) throws NotFoundException {
        logger.debug("delete job");
        try {
            kafkaRegistry.deleteJob(jobId);
            return Response.ok().build();
        } catch (RegistryException e) {
            String error = "unable to get alls job from kafkastore " + e;
            logger.error(error);
            return Response.serverError().entity(error).build();
        }
    }

    @Override
    public Response getAllJobs(SecurityContext securityContext) throws NotFoundException {

        logger.debug("get all jobs");
        try {
            List<Job> jobs = kafkaRegistry.getAllJobs();
            return Response.ok().entity(jobs).build();
        } catch (RegistryException e) {
            String error = "unable to get alls job from kafkastore " + e;
            logger.error(error);
            return Response.serverError().entity(error).build();
        }

    }

    @Override
    public Response getJob(String jobId, SecurityContext securityContext) throws NotFoundException {

        logger.debug("get job " + jobId);


        Job job = null;
        try {
            job = kafkaRegistry.getJob(jobId);
        } catch (RegistryException e) {
            return Response.serverError().entity(e).build();
        }

        if (job == null)
            return Response.serverError()
                    .status(Response.Status.NOT_FOUND)
                    .entity(new Error().code(404).message("Job not found for id: " + jobId))
                    .build();
        else
            return Response.ok().entity(job).build();
    }

    @Override
    public Response getJobAlerts(Integer count, SecurityContext securityContext) throws NotFoundException {
        return null;
    }

    @Override
    public Response getJobErrors(Integer count, SecurityContext securityContext) throws NotFoundException {
        return null;
    }

    @Override
    public Response getJobMetrics(Integer count, SecurityContext securityContext) throws NotFoundException {
        return null;
    }


    //------------------------
    //  Jobs metrology
    //------------------------




    //------------------------
    // Jobs scheduling
    //------------------------

    @Override
    public Response getJobStatus(String jobId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }

    @Override
    public Response pauseJob(String jobId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }

    @Override
    public Response shutdownJob(String jobId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }

    @Override
    public Response startJob(String jobId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }

}
