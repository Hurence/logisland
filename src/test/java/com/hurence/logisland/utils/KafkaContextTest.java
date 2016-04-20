package com.hurence.logisland.utils;

import org.junit.Test;

import static org.junit.Assert.*;

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