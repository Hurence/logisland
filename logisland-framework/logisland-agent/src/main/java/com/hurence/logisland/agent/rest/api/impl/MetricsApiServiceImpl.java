package com.hurence.logisland.agent.rest.api.impl;

import com.hurence.logisland.agent.rest.api.*;
import com.hurence.logisland.agent.rest.model.*;

import com.hurence.logisland.agent.rest.model.Error;

import java.util.List;

import com.hurence.logisland.agent.rest.api.NotFoundException;

import java.io.InputStream;

import com.hurence.logisland.kafka.registry.KafkaRegistry;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-07-28T16:23:56.034+02:00")
public class MetricsApiServiceImpl extends MetricsApiService {

    public MetricsApiServiceImpl(KafkaRegistry kafkaRegistry) {
        super(kafkaRegistry);
    }

    @Override
    public Response getMetrics(SecurityContext securityContext) throws NotFoundException {

        String result = "# HELP http_requests_total The total number of HTTP requests.\n" +
                "# TYPE http_requests_total counter\n" +
                "http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n" +
                "http_requests_total{method=\"post\",code=\"400\"}    3 1395066363000\n" +
                "\n" +
                "# Escaping in label values:\n" +
                "msdos_file_access_time_seconds{path=\"C:\\\\DIR\\\\FILE.TXT\",error=\"Cannot find file:\\n\\\"FILE.TXT\\\"\"} 1.458255915e9\n" +
                "\n" +
                "# Minimalistic line:\n" +
                "metric_without_timestamp_and_labels 12.47\n" +
                "\n" +
                "# A weird metric from before the epoch:\n" +
                "something_weird{problem=\"division by zero\"} +Inf -3982045";

        return Response.ok()
                .type("text/plain")
                .entity(result)
                .build();
    }
}
