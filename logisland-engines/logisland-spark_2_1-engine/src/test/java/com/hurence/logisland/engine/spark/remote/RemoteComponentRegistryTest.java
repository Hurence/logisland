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

import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.MockProcessingEngine;
import com.hurence.logisland.engine.StandardEngineContext;
import com.hurence.logisland.engine.spark.remote.mock.MockProcessor;
import com.hurence.logisland.engine.spark.remote.mock.MockServiceController;
import com.hurence.logisland.engine.spark.remote.mock.MockStream;
import com.hurence.logisland.engine.spark.remote.model.Pipeline;
import com.hurence.logisland.engine.spark.remote.model.Processor;
import com.hurence.logisland.engine.spark.remote.model.Service;
import com.hurence.logisland.engine.spark.remote.model.Stream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;

public class RemoteComponentRegistryTest {

    private static final Logger logger = LoggerFactory.getLogger(RemoteComponentRegistryTest.class);


    @Test
    public void updateEngineContext() {
        MockProcessingEngine engine = new MockProcessingEngine();
        EngineContext masterContext = new StandardEngineContext(engine, "master");
        engine.start(masterContext);
        RemoteComponentRegistry registry = new RemoteComponentRegistry(masterContext);
        Pipeline pipeline1 = createPipeline("pipeline1");
        Pipeline pipeline2 = createPipeline("pipeline2");
        logger.info("should create two new pipelines");
        registry.updateEngineContext(Arrays.asList(pipeline1, pipeline2));
        logger.info("should do nothing");
        registry.updateEngineContext(Arrays.asList(pipeline1, pipeline2));
        logger.info("should remove a pipeline");
        registry.updateEngineContext(Arrays.asList(pipeline1));
        logger.info("should update the pipeline (since touch is fresher)");
        pipeline1 = createPipeline("pipeline1");
        registry.updateEngineContext(Arrays.asList(pipeline1));
        logger.info("should do nothing");
        registry.updateEngineContext(Arrays.asList(pipeline1));
        logger.info("should remove everything (Stop)");
        registry.updateEngineContext(Collections.emptyList());
        engine.shutdown(masterContext);


    }

    private Service createService(String name) {
        return (Service) new Service()
                .name(name)
                .component(MockServiceController.class.getCanonicalName());
    }

    private Processor createProcessor(String name) {
        return (Processor) new Processor()
                .name(name)
                .component(MockProcessor.class.getCanonicalName());
    }

    private Stream createStream(String name) {
        return (Stream) new Stream()
                .addProcessorsItem(createProcessor("processor1"))
                .addProcessorsItem(createProcessor("processor2"))
                .name(name)
                .component(MockStream.class.getCanonicalName());

    }

    private Pipeline createPipeline(String name) {
        return new Pipeline()
                .addServicesItem(createService("service"))
                .addStreamsItem(createStream("stream1"))
                .lastModified(OffsetDateTime.now())
                .name(name);
    }
}