package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.client.exceptions.RestClientException;
import com.hurence.logisland.agent.rest.model.Property;

import java.util.List;

public interface ConfigsApiClient {

    List<Property> getConfigs() throws RestClientException;
}
