package com.hurence.logisland.integration;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * Created by lhubert on 15/04/16.
 */
public class KafkaContextTest {

    @Test
    public void KafkaContextCreation() {
        KafkaContext context = new KafkaContext();
        assertTrue(context.getZkClient() != null);
    }

}