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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hurence.logisland.engine.spark.remote.model.DataFlow;
import okhttp3.*;
import okhttp3.internal.http.HttpDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Rest client wrapper for logisland remote APIs.
 *
 * @author amarziali
 */
public class RemoteApiClient {

    /**
     * Conversation state.
     */
    public static class State {
        public Instant lastModified;
    }

    /**
     * Connection settings.
     */
    public static class ConnectionSettings {

        private final String baseUrl;
        private final Duration socketTimeout;
        private final Duration connectTimeout;
        private final String username;
        private final String password;

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
        public ConnectionSettings(String baseUrl, Duration socketTimeout, Duration connectTimeout, String username, String password) {
            this.baseUrl = baseUrl;
            this.socketTimeout = socketTimeout;
            this.connectTimeout = connectTimeout;
            this.username = username;
            this.password = password;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(RemoteApiClient.class);

    private static final String DATAFLOW_RESOURCE_URI = "dataflows";
    private static final String STREAM_RESOURCE_URI = "streams";


    private static final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private final OkHttpClient client;
    private final HttpUrl baseUrl;
    private final ObjectMapper mapper;


    /**
     * Construct a new instance with provided connection settings.
     *
     * @param connectionSettings the {@link ConnectionSettings}
     */
    public RemoteApiClient(ConnectionSettings connectionSettings) {
        this.baseUrl = HttpUrl.parse(connectionSettings.baseUrl);
        this.mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.registerModule(new JavaTimeModule())
                .findAndRegisterModules();

        OkHttpClient.Builder builder = new OkHttpClient()
                .newBuilder()
                .readTimeout(connectionSettings.socketTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .writeTimeout(connectionSettings.socketTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .connectTimeout(connectionSettings.connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .followRedirects(true)
                .followSslRedirects(true);
        //add basic auth if needed.
        if (connectionSettings.username != null && connectionSettings.password != null) {
            builder.addInterceptor(chain -> {
                Request originalRequest = chain.request();
                Request requestWithBasicAuth = originalRequest
                        .newBuilder()
                        .header(HttpHeaders.AUTHORIZATION, Credentials.basic(connectionSettings.username, connectionSettings.password))
                        .build();
                return chain.proceed(requestWithBasicAuth);
            });
        }
        this.client = builder.build();
    }


    /**
     * Generic method to fetch and validate a HTTP resource.
     *
     * @param url           the resource Url.
     * @param state         the conversation state.
     * @param resourceClass the bean model class.
     * @param <T>           the type of the model data to return.
     * @return an {@link Optional} bean containing requested validated data.
     */
    private <T> Optional<T> doFetch(HttpUrl url, State state, Class<T> resourceClass) {
        Request.Builder request = new Request.Builder()
                .url(url).addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);

        if (state.lastModified != null) {
            request.addHeader(HttpHeaders.IF_MODIFIED_SINCE, HttpDate.format(new Date((state.lastModified.toEpochMilli()))));
        }

        try (Response response = client.newCall(request.build()).execute()) {
            if (response.code() != javax.ws.rs.core.Response.Status.NOT_MODIFIED.getStatusCode()) {

                if (!response.isSuccessful()) {
                    logger.error("Error refreshing {} from remote server. Got code {}", resourceClass.getCanonicalName(), response.code());
                } else {
                    String lm = response.header(HttpHeaders.LAST_MODIFIED);
                    if (lm != null) {
                        try {
                            Date tmp = HttpDate.parse(lm);
                            if (tmp != null) {
                                state.lastModified = tmp.toInstant();
                            }
                        } catch (Exception e) {
                            logger.warn("Unable to correctly parse Last-Modified Header");
                        }
                    }
                    T ret = mapper.readValue(response.body().byteStream(), resourceClass);
                    //validate against javax.validation annotations.
                    doValidate(ret);
                    return Optional.of(ret);
                }
            }
        } catch (Exception e) {
            logger.error("Unable to refresh dataflow from remote server", e);
        }

        return Optional.empty();
    }

    /**
     * Perform validation of the given bean.
     *
     * @param bean the instance to validate
     * @see javax.validation.Validator#validate
     */
    private void doValidate(Object bean) {
        Set<ConstraintViolation<Object>> result = validator.validate(bean);
        if (!result.isEmpty()) {
            StringBuilder sb = new StringBuilder("Bean validation failed: ");
            for (Iterator<ConstraintViolation<Object>> it = result.iterator(); it.hasNext(); ) {
                ConstraintViolation<Object> violation = it.next();
                sb.append(violation.getPropertyPath()).append(" - ").append(violation.getMessage());
                if (it.hasNext()) {
                    sb.append("; ");
                }
            }
            throw new ConstraintViolationException(sb.toString(), result);
        }
    }

    /**
     * Fetches dataflow from a remote server.
     *
     * @param dataflowName the name of the dataflow to fetch.
     * @param state        the conversation state (never null)
     * @return a optional {@link DataFlow} (never null). Empty in case of error or no results.
     */
    public Optional<DataFlow> fetchDataflow(String dataflowName, State state) {
        return doFetch(baseUrl.newBuilder().addPathSegment(DATAFLOW_RESOURCE_URI).addPathSegment(dataflowName).build(),
                state, DataFlow.class);
    }

    /**
     * Push a dataflow configuration to a remote server.
     * We do not care about http result code since the call is fire and forget.
     *
     * @param dataflowName the name of the dataflow to push
     * @param dataFlow     the item to push.
     */
    public void pushDataFlow(String dataflowName, DataFlow dataFlow) {
        try {
            Request request = new Request.Builder()
                    .url(baseUrl.newBuilder()
                            .addPathSegment(DATAFLOW_RESOURCE_URI).addPathSegment(dataflowName)
                            .build())
                    .addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON)
                    .post(RequestBody.create(
                            okhttp3.MediaType.parse(MediaType.APPLICATION_JSON),
                            mapper.writeValueAsString(dataFlow)))
                    .build();
            try (Response response = client.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    logger.warn("Expected application to answer with 200 OK. Got {}", response.code());
                }
            }


        } catch (Exception e) {
            logger.warn("Unexpected exception trying to push latest dataflow configuration", e);
        }
    }


}
