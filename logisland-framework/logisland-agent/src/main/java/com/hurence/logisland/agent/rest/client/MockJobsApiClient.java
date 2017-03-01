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

import com.hurence.logisland.agent.rest.model.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * http://www.hascode.com/2013/12/jax-rs-2-0-rest-client-features-by-example/
 */
public class MockJobsApiClient implements JobsApiClient {

    public static final String BASIC_JOB = "basicJob";

    public MockJobsApiClient() {
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
                .addConfigItem(new Property().key("kafka.output.topics").value("apache_records"))
                .addConfigItem(new Property().key("kafka.error.topics").value("_errors"))
                .addProcessorsItem(processor);


        Job job = new Job()
                .id(1234L)
                .name(BASIC_JOB)
                .version(1)
                .engine(engine)
                .addStreamsItem(stream)
                .summary(summary);


        addJob(job);

    }

    Map<String, Job> jobs = new HashMap<>();

    @Override
    public Job addJob(Job job) {
        jobs.put(job.getName(), job);
        return job;
    }

    @Override
    public Job getJob(String name) {
        return jobs.get(name);
    }


}
