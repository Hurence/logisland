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
package com.hurence.logisland.stream.spark.structured.provider;

import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.utils.ConfJobHelper;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ElasticSearchStructuredStreamSinkProviderServiceTest {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchStructuredStreamSinkProviderServiceTest.class);

    private static final String JOB_CONF_FILE = "/conf/structured-stream-es-sink.yml";

    @Test
    @Ignore
    public void indexInLocalES() {

        logger.info("Starting StreamProcessingRunner");

        Optional<EngineContext> engineInstance = Optional.empty();
        try {
            String configFile = ElasticSearchStructuredStreamSinkProviderServiceTest.class.getResource(JOB_CONF_FILE).getPath();
            ConfJobHelper confJob = new ConfJobHelper(configFile);
            confJob.initJob();
            confJob.startThenAwaitTermination();
        } catch (Exception e) {
            logger.error("Unable to launch runner : {}", e);
        }
    }
}