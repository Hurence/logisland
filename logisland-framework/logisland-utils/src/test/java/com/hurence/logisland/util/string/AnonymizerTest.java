/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
