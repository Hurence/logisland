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

package com.hurence.logisland.repository;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class DomainTagsRepositoryTest {
	

	@Test
	public void testFindTagsByDomain() {
		System.out.println("findTagsByDomain");
		String domain = "";
		DomainTagsRepository repository = new DomainTagsRepository();
		repository.load(this.getClass().getResource("/data/url-tags.json").getFile());
		
		
		List<String> expResult = new ArrayList<>();
		expResult.add("tracker");
		expResult.add("Google");
		
		List<String> result = repository.findTagsByDomain("gstatic.com");
		assertEquals(expResult, result);
	}
	
}
