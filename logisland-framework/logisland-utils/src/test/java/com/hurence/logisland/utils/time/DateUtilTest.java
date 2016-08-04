/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.utils.time;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.Assert.*;

/**
 * @author tom
 */
public class DateUtilTest {

    private static final TimeZone tz = TimeZone.getTimeZone("Europe/Paris");

    private static Logger logger = LoggerFactory.getLogger(DateUtilTest.class);

    // TODO fix test here on unix

    /**
     * org.junit.ComparisonFailure: expected:<2012-10-19T[18]:12:49.000 CEST> but was:<2012-10-19T[20]:12:49.000 CEST>
     * at org.junit.Assert.assertEquals(Assert.java:115)
     * at org.junit.Assert.assertEquals(Assert.java:144)
     * at com.hurence.logisland.utils.time.DateUtilsTest.testDateToString(DateUtilsTest.java:33)
     *
     * @throws ParseException
     */
    //@Test
    public void testDateToString() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("fr", "FR"));
        Date date = sdf.parse("19/Oct/2012:18:12:49");
        String result = DateUtil.toString(date);
        String expectedResult = "2012-10-19T18:12:49.000 CEST";

        assertEquals(expectedResult, result);
    }

    @Test
    public void testStringToDate() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("en", "US"));
        sdf.setTimeZone(tz);
        Date expectedResult = sdf.parse("19/Oct/2012:18:12:49");
        String dateString = "2012-10-19T18:12:49.000 CEST";
        Date result = DateUtil.fromIsoStringToDate(dateString);
        assertEquals(expectedResult, result);

        Date now = new Date();
        assertEquals(now, DateUtil.fromIsoStringToDate(DateUtil.toString(now)));
    }

    @Test
    public void testIsWeekEnd() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWeekend(date));
        date = sdf.parse("28/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWeekend(date));
        date = sdf.parse("29/Jul/2013:00:00:01");
        assertFalse(DateUtil.isWeekend(date));
        date = sdf.parse("26/Jul/2013:23:59:59");
        assertFalse(DateUtil.isWeekend(date));
    }

    @Test
    public void testIsWithinRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtil.isWithinHourRange(date, 9, 19));
        assertFalse(DateUtil.isWithinHourRange(date, 19, 20));
    }

    @Test
    public void testTimeout() throws InterruptedException {

        Date date = new Date();

        // go in the past from 5"
        date.setTime(date.getTime() - 2000);

        assertFalse(DateUtil.isTimeoutExpired(date, 3000));

        Thread.sleep(2000);
        assertTrue(DateUtil.isTimeoutExpired(date, 3000));
    }


    @Test
    public void testLegacyFormat() throws ParseException {
        String strDate = "Thu Jan 02 08:43:49 CET 2014";
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy", new Locale("en", "US"));
        Date date = sdf.parse(strDate);
        assertTrue(1388648629000L == date.getTime());

        date = DateUtil.fromLegacyStringToDate(strDate);
        assertTrue(1388648629000L == date.getTime());
    }

    @Test
    public void testGetLatestDaysFromNow() throws ParseException {
        String result = DateUtil.getLatestDaysFromNow(3);
        //  String expectedResult = "2012-10-19T18:12:49.000 CEST";

        // assertEquals(expectedResult, result);
    }

    @Test
    public void testParsing() throws ParseException {
        String[] strDates = {
                "Thu Jan 02 08:43:49 CET 2014",
                "Thu, 02 Jan 2014 08:43:49 CET",
                "2014-01-02T08:43:49CET",
                "2014-01-02T08:43:49.000CET",
                "2014-01-02T08:43:49.0000+01:00",
                "2014-01-02 08:43:49",
                "2014-01-02 08:43:49,000"
        };



        for (String strDate : strDates) {
            Date date = DateUtil.parse(strDate);
            assertTrue(1388648629000L == date.getTime());

        }


    }

}
