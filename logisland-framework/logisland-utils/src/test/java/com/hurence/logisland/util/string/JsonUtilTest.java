/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.hurence.logisland.util.string;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class JsonUtilTest {



	/**
	 * Test of convertJsonToList method, of class JsonUtil.
	 */
	@Test
	public void testConvertJsonToList() {
		System.out.println("convertJsonToList");
		String json = "[\"media\",\"social\",\"api\",\"Google\"]";
		List<String> expResult = new ArrayList<>();
		expResult.add("media");
		expResult.add("social");
		expResult.add("api");
		expResult.add("Google");
		List<String> result = JsonUtil.convertJsonToList(json);
		assertEquals(expResult, result);
	}


	
}
