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
package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.model.Job;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;


/**
 * http://www.hascode.com/2013/12/jax-rs-2-0-rest-client-features-by-example/
 */
public class RestJobsApiClient implements JobsApiClient {

    Client client = ClientBuilder.newClient().register(JacksonFeature.class);
    private final String restServiceUrl;
    private static Logger logger = LoggerFactory.getLogger(RestJobsApiClient.class);

    public RestJobsApiClient() {
        this("http://localhost:8081");
    }

    public RestJobsApiClient(String baseUrl) {

        this.restServiceUrl = baseUrl + "/jobs";
    }

    @Override
    public Job addJob(Job job) {
        return client.target(restServiceUrl)
                .request()
                .post(Entity.entity(job, MediaType.APPLICATION_JSON), Job.class);

    }

    @Override
    public Job getJob(String name) {

        Job job = null;
        try {
            job = client.target(restServiceUrl)
                    .path("/{jobId}")
                    .resolveTemplate("jobId", name)
                    .request()
                    .get(Job.class);
        } catch (Exception ex) {
            logger.debug("unable to get Job {} from REST agent", name);
        }
        return job;

    }

    @Override
    public Integer getJobVersion(String name) {
        Job job = getJob(name);
        if (job == null)
            return -1;
        else
            return job.getVersion();
    }
}
