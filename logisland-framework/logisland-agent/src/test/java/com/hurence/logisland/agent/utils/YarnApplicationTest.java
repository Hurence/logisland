package com.hurence.logisland.agent.utils;

import org.junit.Assert;
import org.junit.Test;


public class YarnApplicationTest {


    private static String sample = " application_1484246503127_0024\t          SaveToHDFS\t               SPARK\t   hurence\t   default\t           RUNNING\t         UNDEFINED\t            10%\t           http://10.91.84.219:4051\n";

    @Test
    public void construct() throws Exception {
        YarnApplication app = new YarnApplication(sample);
        Assert.assertEquals(app.getId(), "application_1484246503127_0024");
        Assert.assertEquals(app.getName(), "SaveToHDFS");
        Assert.assertEquals(app.getType(), "SPARK");
        Assert.assertEquals(app.getUser(), "hurence");
        Assert.assertEquals(app.getYarnQueue(), "default");
        Assert.assertEquals(app.getState(), "RUNNING");
        Assert.assertEquals(app.getFinalState(), "UNDEFINED");
        Assert.assertEquals(app.getProgress(), "10%");
        Assert.assertEquals(app.getTrackingUrl(), "http://10.91.84.219:4051");
    }

}