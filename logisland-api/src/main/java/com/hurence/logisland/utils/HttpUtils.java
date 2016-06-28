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

package com.hurence.logisland.utils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tom
 */
public class HttpUtils {

	private static final Logger log = LoggerFactory.getLogger(DateUtils.class);

	private static class QueryParam {

		private String key;
		private String value;
	}

	/**
	 * Take a url query string and split it into a map of key/values map
	 *
	 * @param urlQuery
	 * @return
	 */
	public static List<String> getQueryKeys(String urlQuery) {
		try {
			List<String> keys = new ArrayList<>();
			if (urlQuery == null) {
				return keys;
			}

			for (String param : urlQuery.split("&")) {
				QueryParam queryParam = new QueryParam();
				String[] pair = param.split("=");
				String key = URLDecoder.decode(pair[0], "UTF-8");
				if (key != null && !key.equals("null")) {
					keys.add(key);
				}
			}
			return keys;
		} catch (UnsupportedEncodingException ex) {
			throw new AssertionError(ex);
		}
	}

	public static List<String> getQueryValues(String urlQuery) {
		try {
			List<String> values = new ArrayList<>();
			if (urlQuery == null) {
				return values;
			}

			for (String param : urlQuery.split("&")) {
				QueryParam queryParam = new QueryParam();
				String[] pair = param.split("=");
				String value = "";
				if (pair.length > 1) {
					value = URLDecoder.decode(pair[1], "UTF-8");
				}
				if (value != null && !value.equals("null")) {
					values.add(value);
				}
			}
			return values;
		} catch (UnsupportedEncodingException ex) {
			throw new AssertionError(ex);
		}
	}

	public static void populateKeyValueListFromUrlQuery(String urlQuery, List<String> keys, List<String> values) {
		try {
			if (urlQuery == null) {
				return;
			}

			for (String param : urlQuery.split("&")) {

				try {
					QueryParam queryParam = new QueryParam();
					String[] pair = param.split("=");
					if (pair.length == 0) {
						continue;
					}

					String value = "";
					String key = URLDecoder.decode(pair[0], "UTF-8");
					if (key != null && !key.equals("null")) {
						keys.add(key);
					}

					if (pair.length > 1) {
						value = URLDecoder.decode(pair[1], "UTF-8");
					}
					if (value != null && !value.equals("null")) {
						values.add(value);
					}
				} catch (IllegalArgumentException ex) {
					log.debug("error decoding value : " + ex);
				}
			}
		} catch (UnsupportedEncodingException ex) {
			throw new AssertionError(ex);
		}
	}

	public static URI getURIFromEncodedString(String unencoded) throws URISyntaxException, UnsupportedEncodingException {

		URI uri;

		String[] split = unencoded.split("\\?");
		String part1 = split[0]
				.replaceAll("\\[", URLEncoder.encode("[", "UTF8"))
				.replaceAll("\\]", URLEncoder.encode("]", "UTF8"))
				.replaceAll("\\|", URLEncoder.encode("|", "UTF8"));

		if (split.length < 2) {
			uri = new URI(part1);
		} else {

			uri = new URI(part1 + "?"
					+ URLEncoder.encode(split[1], "UTF8"));
		}

		return uri;
	}
}
