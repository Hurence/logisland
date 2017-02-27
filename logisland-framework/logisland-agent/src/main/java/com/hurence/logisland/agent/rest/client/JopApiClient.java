/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.agent.rest.client;

import com.hurence.logisland.agent.rest.api.JobsApi;
import com.hurence.logisland.agent.rest.model.*;
import org.glassfish.jersey.jackson.JacksonFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.util.Date;


/**
 * http://www.hascode.com/2013/12/jax-rs-2-0-rest-client-features-by-example/
 */
public class JopApiClient {

    Client client = ClientBuilder.newClient().register(JacksonFeature.class);
    private static String REST_SERVICE_URL = "http://localhost:8081/jobs";

    public Job addJob(Job job){
        return client.target(REST_SERVICE_URL)
                .request()
                .post(Entity.entity(job, MediaType.APPLICATION_JSON), Job.class);

    }

    public Job getJob(String name){
        return client.target(REST_SERVICE_URL)
                .path("/{jobId}")
                .resolveTemplate("jobId", name)
                .request()
                .get(Job.class);

    }

    public static void main(String[] args) {

        Engine engine = new Engine()
                .name("apache parser engine")
                .component("com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine")
                .addConfigItem(new Property().key("spark.master").value("local[4]"))
                .addConfigItem(new Property().key("spark.streaming.batchDuration").value("4000"));

        JobSummary summary = new JobSummary()
                .dateModified(new Date())
                .documentation("sample job")
                .status(JobSummary.StatusEnum.RUNNING)
                .usedCores(2)
                .usedMemory(24);


        Processor processor = new Processor()
                .name("apacheParser")
                .component("com.hurence.logisland.processor.SplitText")
                .addConfigItem(new Property().key("record_type").value("apache_log"))
                .addConfigItem(new Property().key("value.regex").value("(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s*(\\S*)\"\\s+(\\S+)\\s+(\\S+)"))
                .addConfigItem(new Property().key("value.fields").value("src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out"));



        Stream stream = new Stream()
                .name("apacheStream")
                .component("com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing")
                .addConfigItem(new Property().key("kafka.input.topics").value("apache_raw"))
                .addConfigItem(new Property().key("kafka.output.topics").value("_records"))
                .addConfigItem(new Property().key("kafka.error.topics").value("_errors"))
                .addConfigItem(new Property().key("kafka.input.topics.serializer").value("none"))
                .addConfigItem(new Property().key("kafka.output.topics.serializer").value("com.hurence.logisland.serializer.KryoSerializer"))
                .addConfigItem(new Property().key(" kafka.error.topics.serializer").value("com.hurence.logisland.serializer.JsonSerializer"))
                .addProcessorsItem(processor);


        Job job = new Job()
                .name("testJob3")
                .version(2)
                .engine(engine)
                .addStreamsItem(stream)
                .summary(summary);


        new JopApiClient().addJob(job);
        Job jobadded = new JopApiClient().getJob("testJob3");

        System.out.println("jobPersisted = " + jobadded);
    }
}
