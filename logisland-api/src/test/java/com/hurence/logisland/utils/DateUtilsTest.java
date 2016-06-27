/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class DateUtilsTest {

    private static final TimeZone tz = TimeZone.getTimeZone("Europe/Paris");

    @Test
    public void testDateToString() throws ParseException {
        System.out.println("toString");
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("19/Oct/2012:18:12:49");
        String result = DateUtils.toString(date);
        String expectedResult = "2012-10-19T18:12:49.000 CEST";

        assertEquals(expectedResult, result);
    }

    @Test
    public void testStringToDate() throws ParseException {
        System.out.println("toString");
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("en", "US"));
        sdf.setTimeZone(tz);
        Date expectedResult = sdf.parse("19/Oct/2012:18:12:49");
        String dateString = "2012-10-19T18:12:49.000 CEST";
        Date result = DateUtils.fromIsoStringToDate(dateString);
        assertEquals(expectedResult, result);

        Date now = new Date();
        assertEquals(now, DateUtils.fromIsoStringToDate(DateUtils.toString(now)));
    }

    @Test
    public void testIsWeekEnd() throws ParseException {
        System.out.println("testIsWeekEnd");
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtils.isWeekend(date));
        date = sdf.parse("28/Jul/2013:18:12:49");
        assertTrue(DateUtils.isWeekend(date));
        date = sdf.parse("29/Jul/2013:00:00:01");
        assertFalse(DateUtils.isWeekend(date));
        date = sdf.parse("26/Jul/2013:23:59:59");
        assertFalse(DateUtils.isWeekend(date));
    }

    @Test
    public void testIsWithinRange() throws ParseException {
        System.out.println("testIsWithinRange");
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
        Date date = sdf.parse("27/Jul/2013:18:12:49");
        assertTrue(DateUtils.isWithinHourRange(date, 9, 19));
        assertFalse(DateUtils.isWithinHourRange(date, 19, 20));
    }

    @Test
    public void testTimeout() throws InterruptedException {

        Date date = new Date();

        // go in the past from 5"
        date.setTime(date.getTime() - 2000);

        assertFalse(DateUtils.isTimeoutExpired(date, 3000));

        Thread.sleep(2000);
        assertTrue(DateUtils.isTimeoutExpired(date, 3000));
    }
    
    
    @Test
    public void testLegacyFormat() throws ParseException {
        System.out.println("testLegacyFormat");
        String strDate = "Thu Jan 02 08:43:49 CET 2014";
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy", new Locale("en", "US"));
        Date date = sdf.parse( strDate);
        assertTrue(1388648629000L == date.getTime());
        
        
        
        date = DateUtils.fromLegacyStringToDate(strDate);
        assertTrue(1388648629000L == date.getTime());
        
    }
    
    @Test
    public void testGetLatestDaysFromNow() throws ParseException {
        System.out.println("toString");

        String result = DateUtils.getLatestDaysFromNow(3);
      //  String expectedResult = "2012-10-19T18:12:49.000 CEST";

       // assertEquals(expectedResult, result);
    }
   
}
