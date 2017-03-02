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
import com.hurence.logisland.processor.MockProcessor;
import com.hurence.logisland.processor.SplitText;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * http://www.hascode.com/2013/12/jax-rs-2-0-rest-client-features-by-example/
 */
public class MockJobsApiClient implements JobsApiClient {

    public static final String MAGIC_STRING = "the world is so big";
    public static final String APACHE_PARSING_JOB = "apache_parsing_job";
    public static final String MOCK_PROCESSING_JOB = "mock_processing_job";

    public MockJobsApiClient() {
        Engine engine = new Engine()
                .name("apache parser engine")
                .component("com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine")
                .addConfigItem(new Property().key("spark.master").value("local[4]"))
                .addConfigItem(new Property().key("spark.streaming.batchDuration").value("500"))
                .addConfigItem(new Property().key("spark.streaming.timeout").value("12000"));

        JobSummary summary = new JobSummary()
                .dateModified(new Date())
                .documentation("sample job")
                .status(JobSummary.StatusEnum.RUNNING)
                .usedCores(2)
                .usedMemory(24);


        Processor apacheParserProcessor = new Processor()
                .name("apacheParser")
                .component(SplitText.class.getCanonicalName())
                .addConfigItem(new Property()
                        .key(SplitText.RECORD_TYPE.getName())
                        .value("apache_log"))
                .addConfigItem(new Property()
                        .key(SplitText.VALUE_REGEX.getName())
                        .value("(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+\\[([\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s+\"(\\S+)\\s+(\\S+)\\s*(\\S*)\"\\s+(\\S+)\\s+(\\S+)"))
                .addConfigItem(new Property()
                        .key(SplitText.VALUE_FIELDS.getName())
                        .value("src_ip,identd,user,record_time,http_method,http_query,http_version,http_status,bytes_out"));


        addJob(new Job()
                .id(1234L)
                .name(APACHE_PARSING_JOB)
                .version(1)
                .engine(engine)
                .addStreamsItem(new Stream()
                        .name("apacheStream")
                        .component("com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing")
                        .addConfigItem(new Property().key("kafka.input.topics").value("apache_raw"))
                        .addConfigItem(new Property().key("kafka.output.topics").value("apache_records"))
                        .addConfigItem(new Property().key("kafka.error.topics").value("_errors"))
                        .addProcessorsItem(apacheParserProcessor))
                .summary(summary));

        addJob(new Job()
                .id(1235L)
                .name(MOCK_PROCESSING_JOB)
                .version(1)
                .engine(engine)
                .addStreamsItem(new Stream()
                        .name("mockStream")
                        .component("com.hurence.logisland.stream.spark.KafkaRecordStreamParallelProcessing")
                        .addConfigItem(new Property().key("kafka.input.topics").value("mock_in"))
                        .addConfigItem(new Property().key("kafka.output.topics").value("mock_out"))
                        .addConfigItem(new Property().key("kafka.error.topics").value("_errors"))
                        .addProcessorsItem(new Processor()
                                .name("mockProcessor")
                                .component(MockProcessor.class.getCanonicalName())
                                .addConfigItem(new Property()
                                        .key(MockProcessor.FAKE_MESSAGE.getName())
                                        .value(MAGIC_STRING))))
                .summary(summary));

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

    @Override
    public Integer getJobVersion(String name) {
        return getJob(name).getVersion();
    }


}
