/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
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
