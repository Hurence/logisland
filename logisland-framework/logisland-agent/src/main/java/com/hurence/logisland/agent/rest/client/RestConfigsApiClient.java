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

import com.hurence.logisland.agent.rest.model.Property;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;


/**
 * http://www.hascode.com/2013/12/jax-rs-2-0-rest-client-features-by-example/
 */
public class RestConfigsApiClient implements ConfigsApiClient {

    Client client = ClientBuilder.newClient().register(JacksonFeature.class);
    private final String restServiceUrl;

    public RestConfigsApiClient() {
        this("http://localhost:8081");
    }

    public RestConfigsApiClient(String baseUrl) {
        this.restServiceUrl = baseUrl + "/configs";
    }


    @Override
    public List<Property> getConfigs() {
        List<HashMap<String, String>> props = client.target(restServiceUrl)
                .request()
                .get(List.class);

        if (props != null)
            return props.stream()
                    .map(prop -> new Property().key(prop.get("key")).value(prop.get("value")).type(prop.get("type")))
                    .collect(Collectors.toList());
        else return null;
    }
}
