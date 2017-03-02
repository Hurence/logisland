package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.model.Job;

public interface JobsApiClient {
    Job addJob(Job job);

    Job getJob(String name);

    Integer getJobVersion(String name);
}
