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
package com.hurence.logisland.repository.json;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tom
 */
public class JsonTagsFileParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonTagsFileParser.class);

    /**
     * parses the file and returns a dictionnary of domain / tags
     *
     * @param filename
     * @return
     */
    public Map<String, List<String>> parse(String filename) {

        Map<String, List<String>> repository = new HashMap<>();
        JSONParser parser = new JSONParser();
        int domainCount = 0;

        try {

            logger.debug("parsing json file : " + filename);
            Object obj = parser.parse(new FileReader(filename));

            JSONArray domains = (JSONArray) obj;

            for (Object object : domains) {
                domainCount++;
                JSONObject jsonObject = (JSONObject) object;
                String domain = (String) jsonObject.get("term");

                JSONArray tags = (JSONArray) jsonObject.get("tags");

                String company = (String) jsonObject.get("company");
                if (company != null) {
                    tags.add(company);
                }

                repository.put(domain, tags);
            }

            logger.debug("succesfully parsed " + domainCount + "domains");

        } catch (ParseException | IOException ex) {
            logger.error(ex.getMessage());
        }

        return repository;

    }
}
