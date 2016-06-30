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
