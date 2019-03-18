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

import com.hurence.logisland.repository.json.JsonTagsFileParser;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tom
 */
public class DomainTagsRepository {

	private Map<String, List<String>> repository;
	private static final String DOMAIN_TAGS_FILE_NAME = "url-tags.json";

	private static final Logger logger = LoggerFactory.getLogger(DomainTagsRepository.class);

	public DomainTagsRepository(String dbFolderPath) {
		if (dbFolderPath.isEmpty()) {
			throw new IllegalArgumentException("domain url tags db folder cannot be empty");
		}

		// load commons tags domains
		load(dbFolderPath + "/" + DOMAIN_TAGS_FILE_NAME);
	}

	public DomainTagsRepository() {
	}

	public final void load(String filename) {
		logger.debug("loading tags repository from " + filename);
		JsonTagsFileParser parser = new JsonTagsFileParser();
		repository = parser.parse(filename);

	}

	public List<String> findTagsByDomain(String domain) {
		return repository.get(domain);
	}
}
