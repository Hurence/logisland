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
