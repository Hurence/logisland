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
package com.caseystella.analytics.converters.csv;

import com.caseystella.analytics.converters.Converter;
import com.caseystella.analytics.converters.MappingConverter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CSVConverter implements MappingConverter, Serializable {
    static final long serialVersionUID = 1L;
    public static final String COLUMN_MAP_CONF = "columnMap";
    public static final String DELIMITER_CONF = "delim";
    private CSVParser parser;
    private Map< String, Integer> columnMap = new HashMap<>();
    public CSVConverter() {

    }

    private void initialize(Map<String, Object> config) {
        char separator = config.containsKey(DELIMITER_CONF)?config.get(DELIMITER_CONF).toString().charAt(0):',';
        parser = new CSVParserBuilder().withSeparator(separator)
                .build();
        columnMap = (Map<String, Integer>) config.get(COLUMN_MAP_CONF);
    }

    @Override
    public Map<String, Object> convert(byte[] in, Map<String, Object> config) {
        if(parser == null) {
           initialize(config) ;
        }
        String line = Bytes.toString(in);
        if(line.trim().startsWith("#")) {
           return null;
        }
        Map<String, Object> ret = new HashMap<>();
        try {
            String[] tokens = parser.parseLine(line);
            for(Map.Entry<String, Integer> kv : columnMap.entrySet()) {
                ret.put(kv.getKey(), tokens[kv.getValue()]);
            }
        } catch (IOException e) {
            return null;
        }
        return ret;
    }
}
