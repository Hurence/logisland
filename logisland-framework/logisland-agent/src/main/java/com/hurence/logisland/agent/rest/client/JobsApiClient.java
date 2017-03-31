package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.client.exceptions.RestClientException;
import com.hurence.logisland.agent.rest.model.Job;

public interface JobsApiClient {
    Job addJob(Job job) throws RestClientException;

    Job getJob(String name) throws RestClientException;

    Integer getJobVersion(String name) throws RestClientException;
}
