package com.hurence.logisland.integration;

import com.hurence.logisland.utils.kafka.EmbeddedKafkaEnvironment;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by lhubert on 15/04/16.
 */
public class EmbeddedKafkaEnvironmentTest {

    @Test
    public void KafkaContextCreation() {
        EmbeddedKafkaEnvironment context = new EmbeddedKafkaEnvironment();
        assertTrue(context.getZkClient() != null);
    }

}