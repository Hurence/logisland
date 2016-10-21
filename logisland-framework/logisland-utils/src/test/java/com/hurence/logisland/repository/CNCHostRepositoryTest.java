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

	private static final String REPOSITORY_PATH = "/data/cnc_malware_samples.csv";

	@Test
	public void testFindByIp() {
		System.out.println("findByIp");
		
		final BlacklistedHostRepository repository = new BlacklistedHostRepository();
		
		repository.load(this.getClass().getResource(REPOSITORY_PATH).getFile(), BlacklistType.CNC);
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