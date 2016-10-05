/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.util.string;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class AnonymizerTest {

	/**
	 * Test of anonymize method, of class Anonymizer.
	 */
	@Test
	public void testAnonymize_Date() {
		System.out.println("anonymize");
		Anonymizer instance = new Anonymizer();

		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", new Locale("en", "US"));
			Date date1 = sdf.parse("19/Oct/2012:18:12:49");
			Date date2 = sdf.parse("20/Oct/2012:18:12:49");
			
			Date date1Anon = instance.anonymize(date1);
			Date date2Anon = instance.anonymize(date2);
			
			long diff1 = date2.getTime() - date1.getTime();
			long diff2 = date2Anon.getTime() - date1Anon.getTime();
			assertNotEquals(date1, date1Anon);
			assertEquals(diff1, diff2);
			
			

		} catch (ParseException ex) {
			Logger.getLogger(AnonymizerTest.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

}
