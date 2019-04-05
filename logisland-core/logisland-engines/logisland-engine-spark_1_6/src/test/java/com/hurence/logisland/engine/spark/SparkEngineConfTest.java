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
package com.hurence.logisland.engine.spark;

import com.hurence.logisland.component.ComponentFactory;
import com.hurence.logisland.component.ComponentType;
import com.hurence.logisland.config.EngineConfiguration;
import com.hurence.logisland.config.ProcessorConfiguration;
import com.hurence.logisland.config.StreamConfiguration;
import com.hurence.logisland.engine.EngineContext;
import com.hurence.logisland.engine.spark.KafkaStreamProcessingEngine;
import com.hurence.logisland.stream.spark.*;
import com.hurence.logisland.util.runner.MockProcessor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SparkEngineConfTest {
    private static Logger logger = LoggerFactory.getLogger(SparkEngineConfTest.class);

    /**
     * testing all value correct (see https://spark.apache.org/docs/1.6.3/submitting-applications.html#master-urls)
     */
    @Test
    public void sparkMasterConfigTest() {
        EngineConfiguration engineConf = getStandardEngineConfiguration();

        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[*]");
        testConfIsValid(engineConf);
        //spark://HOST:PORT
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "spark://045.478.874.4785217");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "spark://aze0484.44-44:089");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "spark://aze0484.44-44");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "spark://htrh");
        testConfIsValid(engineConf);
        //mesos://HOST:PORT
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://zk://cn1:2181,cn2:2181,cn3:2181/mesos");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://zk://cn1:2181,cn2:2181/mesos");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://zk://cn1:2181/mesos");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://207.184.161.138:7077");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://207.184.161.138");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "mesos://gregh:");
        testConfIsNotValid(engineConf);
        //yarn
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "yarn");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "yarn-client");
        testConfIsValid(engineConf);
        engineConf.getConfiguration().put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "yarn-cluster");
    }

    private void testConfIsValid(EngineConfiguration engineConf) {
        Optional<EngineContext> engineInstance = ComponentFactory.getEngineContext(engineConf);
        Assert.assertTrue(engineInstance.isPresent());
        Assert.assertTrue(engineInstance.get().isValid());
        engineInstance.get();
    }

    private void testConfIsNotValid(EngineConfiguration engineConf) {
        Optional<EngineContext> engineInstance = ComponentFactory.getEngineContext(engineConf);
        Assert.assertTrue(engineInstance.isPresent());
        Assert.assertFalse(engineInstance.get().isValid());
        engineInstance.get();
    }

    private EngineConfiguration getStandardEngineConfiguration() {
        Map<String, String> engineProperties = new HashMap<>();
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_APP_NAME().getName(), "testApp");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_BATCH_DURATION().getName(), "5000");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_MASTER().getName(), "local[4]");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_EXECUTOR_CORES().getName(), "4");
        engineProperties.put(KafkaStreamProcessingEngine.SPARK_STREAMING_TIMEOUT().getName(), "-1");

        EngineConfiguration engineConf = new EngineConfiguration();
        engineConf.setComponent(KafkaStreamProcessingEngine.class.getName());
        engineConf.setType(ComponentType.ENGINE.toString());
        engineConf.setConfiguration(engineProperties);
        return engineConf;
    }
}
