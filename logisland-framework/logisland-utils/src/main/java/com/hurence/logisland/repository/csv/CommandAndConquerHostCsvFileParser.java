/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.repository.csv;

import com.hurence.logisland.repository.MalwareHost;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.text.StrTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tom
 */
public class CommandAndConquerHostCsvFileParser extends CsvFileParser<MalwareHost> {

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd_hh:mm");
	private static final Logger log = LoggerFactory.getLogger(CommandAndConquerHostCsvFileParser.class);

	private String cleanup(String str) {
		if (str != null) {
			return str.replace("\"", "");
		} else {
			return null;
		}

	}

	@Override
	public MalwareHost createEntity(StrTokenizer tokenizer) {
		try {
			// date sample : 2013/07/11_02:06
			String strDate = cleanup(tokenizer.nextToken());
			Date updateDate = null;
			if( strDate != null)
				updateDate = sdf.parse(strDate);
			String url = cleanup(tokenizer.nextToken());
			String ip = cleanup(tokenizer.nextToken());
			String host = cleanup(tokenizer.nextToken());
			String malwareBehavior = cleanup(tokenizer.nextToken());

			return new MalwareHost(updateDate, url, ip, host, malwareBehavior);
		} catch (Exception ex) {
			log.error("unable to create entity for tokenizer :" + tokenizer.toString(), ex);
			return null;
		}
	}
}
