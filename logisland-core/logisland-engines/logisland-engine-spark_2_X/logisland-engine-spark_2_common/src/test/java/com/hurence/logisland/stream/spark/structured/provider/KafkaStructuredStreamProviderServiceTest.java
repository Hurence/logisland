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

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.processor.DebugStream;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import org.junit.jupiter.api.Test;

public class KafkaStructuredStreamProviderServiceTest {

    @Test
    public void testConfig() throws InitializationException {
        final KafkaStructuredStreamProviderService kafkaServiceOutput = new KafkaStructuredStreamProviderService();
        // Any processor will do it, we won't use it but we need a real processor to be instantiated
        final TestRunner runner = TestRunners.newTestRunner(new DebugStream());
        runner.addControllerService("kafka_service_out", kafkaServiceOutput);
        runner.assertNotValid(kafkaServiceOutput);
        runner.setProperty(kafkaServiceOutput, KafkaProperties.INPUT_TOPICS(), "fake_topic");
        runner.assertNotValid(kafkaServiceOutput);
        runner.setProperty(kafkaServiceOutput, KafkaStructuredStreamProviderService.OUTPUT_TOPICS_FIELD(), "topic");
        runner.assertValid(kafkaServiceOutput);
        runner.setProperty(kafkaServiceOutput, KafkaProperties.KAFKA_TOPIC_AUTOCREATE(), "false");
        runner.enableControllerService(kafkaServiceOutput);
    }

}
