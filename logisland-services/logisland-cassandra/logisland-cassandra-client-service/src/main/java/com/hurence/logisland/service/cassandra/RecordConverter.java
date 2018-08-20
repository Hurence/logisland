/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.cassandra;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * This class converts a logisland Record to some Cassandra CQL statements
 */
public class RecordConverter {

    public enum CassandraType {
        UUID("uuid"),
        INT("int");

        private final String value;

        CassandraType(String value)
        {
            this.value = value;
        }

        public static CassandraType fromValue(String value) throws Exception {
            switch(value)
            {
                case "uuid":
                    return UUID;
                    case "int":
                        return INT;
                default:
                    throw new Exception("Unsupported cassandra type: " + value);
            }
        }

        public String getValue()
        {
            return value;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(RecordConverter.class.getName());

    public static String convertInsert(Record record, String keyspace, String table) {

        // INSERT INTO <keyspace>.<table> (
        String INSERT_PREFIX = "INSERT INTO " + keyspace + "." + table + " (";
        StringBuffer result = new StringBuffer(INSERT_PREFIX);
        StringBuffer fields = new StringBuffer();
        StringBuffer values = new StringBuffer();

        // Go through each field and prepare the list of fields names as well as the list of field values
        Set<Map.Entry<String, Field>> entrySet = record.getFieldsEntrySet();
        int lastIndex = entrySet.size() - 1;
        int index = 0;
        for (Map.Entry<String, Field> entry : entrySet) {

            // Get and handle field name
            String fieldName = entry.getKey();
            fields.append(fieldName);
            if (index < lastIndex)
            {
                fields.append(",");
            }

            // Get and handle field value
            Object value = entry.getValue().getRawValue();
            values.append("'").append(value.toString()).append("'");
            if (index < lastIndex)
            {
                values.append(",");
            }

            index++;
        }

        // Concat everything
        // INSERT INTO <keyspace>.<table> (user_id, created_date, email, first_name, last_name) VALUES (14c532ac-f5ae-479a-9d0a-36604732e01d, '2018-07-27', 'tim.berglund@datastax.com', 'Tim', 'Berglund')"
        result.append(fields).append(") VALUES (").append(values).append(")");

        String cql = result.toString();

        // TODO: use debug level
        logger.info("Insert CQL for record " + record + ":\n[" + cql + "]");
        System.out.println("Insert CQL for record " + record + ":\n[" + cql + "]");

        return cql;
    }
}
