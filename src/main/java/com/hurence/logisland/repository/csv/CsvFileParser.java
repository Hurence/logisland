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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.text.StrTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * This helper utility class, takes a csv file and converts each line into an
 * abstract entity through the template method createEntity which is defined by
 * subclasses
 *
 * TODO add some test here
 *
 * @author tom
 */
public abstract class CsvFileParser<T> {

	private static final Log logger = LogFactory.getLog(CsvFileParser.class);

	protected String separator = ",";

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	/**
	 * the abstract methos to be defined by subclass to create an entity from a
	 * tokenized line.
	 *
	 * @param line
	 * @return
	 */
	public abstract T createEntity(StrTokenizer line);

	/**
	 * Parse the file given in parameters
	 *
	 * @param filePath
	 * @return
	 */
	public List<T> parseFile(String filePath) {
		List<T> result = new ArrayList<>();

		InputStreamReader isr = null;
		BufferedReader bsr = null;
		try {
			isr = new InputStreamReader(new FileInputStream(filePath), "UTF-8");
			bsr = new BufferedReader(isr);

			logger.debug("start parsing csv file : " + filePath);
			int nblines = 0;
			String line;
			while ((line = bsr.readLine()) != null) {
				// don't parse the first line of csv
				if (nblines != 0) {
					StrTokenizer tokenizer = new StrTokenizer(line, separator);
					tokenizer.setIgnoreEmptyTokens(false);
					T o = createEntity(tokenizer);
					if (o != null) {
						result.add(o);
					}
				} else {
					nblines++;
				}
			}

			logger.debug("done parsing csv file : " + filePath);

		} catch (FileNotFoundException ex) {
			logger.error("file not found : " + filePath);
		} catch (IOException ex) {
			logger.error("unknown error while parsing : " + filePath);
		} finally {
			try {
				if (bsr != null) {
					bsr.close();
				}
			} catch (IOException ex) {
				logger.error("unknown error while parsing : " + filePath);
			}

		}
		return result;

	}
}
