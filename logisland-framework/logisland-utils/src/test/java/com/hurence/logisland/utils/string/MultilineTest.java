package com.hurence.logisland.utils.string;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by tom on 05/07/16.
 */
public class MultilineTest {


    /**
     {
     "rotationPolicy" : {
     "type" : "BY_AMOUNT"
     ,"amount" : 100
     ,"unit" : "POINTS"
     }
     ,"chunkingPolicy" : {
     "type" : "BY_AMOUNT"
     ,"amount" : 10
     ,"unit" : "POINTS"
     }
     }
     */
    @Multiline
    public static String amountConfig;


    /**
     * Test of convertJsonToList method, of class JsonUtil.
     */
    @Test
    public void testMultiline() {
        Assert.assertTrue(amountConfig.contains("\n"));
    }
}
