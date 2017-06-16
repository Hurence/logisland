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
package com.hurence.logisland.repository;

import com.hurence.logisland.repository.csv.DynDnsFileParser;
import com.hurence.logisland.repository.csv.CommandAndConquerHostCsvFileParser;
import com.hurence.logisland.repository.csv.CsvFileParser;
import com.hurence.logisland.repository.csv.MalwareHostCsvFileParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A csv repository to find blacklisted Host from various sources TODO manage db
 * paths within spring context
 *
 * @author tom
 */
public class BlacklistedHostRepository {

	public enum BlacklistType {

		CNC,
		DOMAIN_BLACK_LIST,
		DYNDNS_LIST
	}

	private String filePath;
	private final Map<String, MalwareHost> ipMap = new HashMap<>();
	private final Map<String, MalwareHost> hostMap = new HashMap<>();
	private static final Logger log = LoggerFactory.getLogger(BlacklistedHostRepository.class);

	private static final String BLACKLIST_REPOSITORY_FILE_NAME = "domain_blacklist.txt";
	private static final String DYNDNS_REPOSITORY_FILE_NAME = "dyndns_blacklist.txt";

	/**
	 * Initialize a standard repository from blacklist and dyndns domains
	 *
	 * @param dbFolderPath
	 */
	public BlacklistedHostRepository(String dbFolderPath) {

		if (dbFolderPath.isEmpty()) {
			throw new IllegalArgumentException("blacklisted db folder cannot be empty");
		}

		// load dlacklisted domains
		this.load(dbFolderPath + "/" + BLACKLIST_REPOSITORY_FILE_NAME,
			BlacklistedHostRepository.BlacklistType.DOMAIN_BLACK_LIST);

		// load dyndns domains
		this.load(dbFolderPath + "/" + DYNDNS_REPOSITORY_FILE_NAME,
			BlacklistedHostRepository.BlacklistType.DYNDNS_LIST);

		log.debug("loaded blacklist repository from " + dbFolderPath);
	}

	public BlacklistedHostRepository() {
	}

	/**
	 * Load a blacklist from a given file and type
	 *
	 * @param filePath
	 * @param type
	 */
	public final void load(String filePath, BlacklistType type) {
		CsvFileParser parser;
		switch (type) {
			case CNC:
				parser = new CommandAndConquerHostCsvFileParser();
				break;
			case DOMAIN_BLACK_LIST:
				parser = new MalwareHostCsvFileParser();
				break;
			case DYNDNS_LIST:
				parser = new DynDnsFileParser();
				break;
			default:
				throw new IllegalArgumentException("wrong MalwareType given : " + type);
		}

		this.filePath = filePath;
		List<MalwareHost> blacklistedHosts = parser.parseFile(filePath);
		for (MalwareHost host : blacklistedHosts) {
			ipMap.put(host.getIp(), host);
			hostMap.put(host.getHost(), host);
		}
	}

	public MalwareHost findByIp(String ip) {
		return ipMap.get(ip);
	}

	public MalwareHost findByHost(String hostName) {
		if (hostName.equals("-") || hostName.equals("0")) {
			return null;
		} else {
			return hostMap.get(hostName);
		}
	}
}
