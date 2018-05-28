/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.spark.remote;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hurence.logisland.engine.spark.remote.model.Pipeline;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Rest client wrapper for pipelines remote APIs.
 *
 * @author amarziali
 */
public class RemoteApiClient {

    private static final Logger logger = LoggerFactory.getLogger(RemoteApiClient.class);

    private static final String PIPELINES_RESOURCE_URI = "pipelines";
    private static final CollectionType pipelineType = TypeFactory.defaultInstance().constructCollectionType(List.class, Pipeline.class);

    private final OkHttpClient client;
    private final HttpUrl baseUrl;
    private final ObjectMapper mapper;


    /**
     * Constructs a new instance.
     * If username and password are provided, the client will be configured to supply a basic authentication.
     *
     * @param baseUrl        the base url
     * @param socketTimeout  the read/write socket timeout
     * @param connectTimeout the connection socket timeout
     * @param username       the username if a basic authentication is needed.
     * @param password       the password if a basic authentication is needed.
     */
    public RemoteApiClient(String baseUrl, Duration socketTimeout, Duration connectTimeout,
                           String username, String password) {
        this.baseUrl = HttpUrl.parse(baseUrl);
        this.mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new JavaTimeModule())
                .findAndRegisterModules();

        OkHttpClient.Builder builder = new OkHttpClient()
                .newBuilder()
                .readTimeout(socketTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .writeTimeout(socketTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .connectTimeout(connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .followRedirects(true)
                .followSslRedirects(true);
        //add basic auth if needed.
        if (username != null && password != null) {
            builder.addInterceptor(chain -> {
                Request originalRequest = chain.request();
                Request requestWithBasicAuth = originalRequest
                        .newBuilder()
                        .header(HttpHeaders.AUTHORIZATION, Credentials.basic(username, password))
                        .build();
                return chain.proceed(requestWithBasicAuth);
            });
        }
        this.client = builder.build();
    }

    /**
     * Fetches pipelines from a remote server.
     *
     * @return a list of {@link Pipeline} (never null). Empty in case of error or no results.
     */
    public List<Pipeline> fetchPipelines() {
        Request request = new Request.Builder()
                .url(baseUrl.newBuilder().addPathSegment(PIPELINES_RESOURCE_URI).build())
                .addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                .get()
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.error("Error refreshing pipelines from remote server. Got code {}", response.code());

            }
            return mapper.readValue(response.body().byteStream(), pipelineType);
        } catch (Exception e) {
            logger.error("Unable to refresh pipelines from remote server", e);
        }

        return Collections.emptyList();


    }


}
