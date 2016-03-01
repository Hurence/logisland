/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hurence.logisland.repository;

import com.hurence.logisland.repository.BlacklistedHostRepository.BlacklistType;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tom
 */
public class CNCHostRepositoryTest {

	private static final String REPOSITORY_PATH = "./src/test/resources/data/cnc_malware_samples.csv";

	@Test
	public void testFindByIp() {
		System.out.println("findByIp");
		
		final BlacklistedHostRepository repository = new BlacklistedHostRepository();
		
		repository.load(REPOSITORY_PATH, BlacklistType.CNC);
		String[] ips = {
			"212.227.31.159",
			"188.138.1.9",
			"69.195.68.94",
		};
		
		for (String ip : ips) {
			assertNotNull(repository.findByIp(ip));
		}
	}
}