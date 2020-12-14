package com.hurence.logisland.processor.webAnalytics.util;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class DateParsingTests {

    //        1575158400000    1/12/2019 à 0:00:00
    //        1577836799000    31/12/2019 à 23:59:59
    //        1577836800000    1/1/2020 à 0:00:00
    //        1577840399000    1/1/2020 à 0:59:59
    //        1580515199000    31/1/2020 à 23:59:59
    //        1580515200000    1/2/2020 à 0:00:00
    //        1583020799000    29/2/2020 à 23:59:59
    @Test
    public void testDateFormatters() {
        DateTimeFormatter eventFormatter = DateTimeFormatter.ofPattern("d/M/yyyy à H:mm:ss Z", Locale.ENGLISH);
        SimpleDateFormat sessionsFormatter = new SimpleDateFormat("d/M/yyyy à H:mm:ss Z");
        //        1575158400000    1/12/2019 à 0:00:00
        Assert.assertEquals("1/12/2019 à 1:00:00 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1575158400000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/12/2019 à 1:00:00 +0100",
                sessionsFormatter.format(1575158400000L));
        //        1577836799000    31/12/2019 à 23:59:59
        Assert.assertEquals("1/1/2020 à 0:59:59 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1577836799000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/1/2020 à 0:59:59 +0100",
                sessionsFormatter.format(1577836799000L));
//        1577836800000    1/1/2020 à 0:00:00
        Assert.assertEquals("1/1/2020 à 1:00:00 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1577836800000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/1/2020 à 1:00:00 +0100",
                sessionsFormatter.format(1577836800000L));
//        1577840399000    1/1/2020 à 0:59:59
        Assert.assertEquals("1/1/2020 à 1:59:59 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1577840399000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/1/2020 à 1:59:59 +0100",
                sessionsFormatter.format(1577840399000L));
//        1580515199000    31/1/2020 à 23:59:59
        Assert.assertEquals("1/2/2020 à 0:59:59 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1580515199000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/2/2020 à 0:59:59 +0100",
                sessionsFormatter.format(1580515199000L));
//        1580515200000    1/2/2020 à 0:00:00
        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1580515200000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
                sessionsFormatter.format(1580515200000L));
        //        1583020799000    29/2/2020 à 23:59:59
        Assert.assertEquals("1/3/2020 à 0:59:59 +0100",
                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1583020799000L), ZoneId.systemDefault())));
        Assert.assertEquals("1/3/2020 à 0:59:59 +0100",
                sessionsFormatter.format(1583020799000L));
    }

//    @Test
//    public void testDateFormatters2() {
//        DateTimeFormatter eventFormatter = DateTimeFormatter.ofPattern("d/M/yyyy à H:mm:ss Z", Locale.ENGLISH);
//        SimpleDateFormat sessionsFormatter = new SimpleDateFormat("d/M/yyyy à H:mm:ss Z");
//
//        Assert.assertEquals(Locale.FRANCE ,Locale.getDefault());
//        Locale.setDefault(Locale.ENGLISH);
//        Assert.assertEquals(Locale.ENGLISH ,Locale.getDefault());
//
//        Assert.assertEquals(ZoneId.of("UTC") ,ZoneId.systemDefault());
//
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1580515200000L), ZoneId.systemDefault())));
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                sessionsFormatter.format(1580515200000L));
//
//        Locale.setDefault(Locale.CANADA);
//        Assert.assertEquals(ZoneId.of("UTC") ,ZoneId.systemDefault());
//
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1580515200000L), ZoneId.systemDefault())));
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                sessionsFormatter.format(1580515200000L));
//
//        Locale.setDefault(Locale.JAPAN);
//        Assert.assertEquals(ZoneId.of("UTC") ,ZoneId.systemDefault());
//
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                eventFormatter.format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1580515200000L), ZoneId.systemDefault())));
//        Assert.assertEquals("1/2/2020 à 1:00:00 +0100",
//                sessionsFormatter.format(1580515200000L));
//    }

}
