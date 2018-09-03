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
package com.hurence.logisland.repository.csv;

import com.hurence.logisland.repository.MalwareHost;
import org.apache.commons.lang3.text.StrTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Take a file containing dyn dns domain and build a list of malware host
 * 
 * @author tom
 */
public class DynDnsFileParser extends CsvFileParser<MalwareHost> {

	private static final Logger log = LoggerFactory.getLogger(DynDnsFileParser.class);

	@Override
	public MalwareHost createEntity(StrTokenizer tokenizer) {
		try {
			String host = tokenizer.nextToken();
			String malwareBehavior = "dynamic dns";
			MalwareHost malwareHost = new MalwareHost(null, "", "", host, malwareBehavior);

			return malwareHost;
		} catch (Exception ex) {
			log.error("unable to create entity for tokenizer :" + tokenizer.toString(), ex);
			return null;
		}
	}
}
