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

import com.datastax.driver.core.LocalDate;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class converts a logisland Record to some Cassandra values to insert
 */
public class RecordConverter {

    public enum CassandraType {

        // Subset of native cassandra data types described here: http://cassandra.apache.org/doc/latest/cql/types.html
        UUID("uuid"),
        TEXT("text"),
        DATE("date"),
        TIME("time"),
        TIMESTAMP("timestamp"),
        TINYINT("tinyint"),
        SMALLINT("smallint"),
        INT("int"),
        BIGINT("bigint"),
        VARINT("varint"),
        FLOAT("float"),
        DOUBLE("double"),
        DECIMAL("decimal"),
        BOOLEAN("boolean"),
        BLOB("blob");

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
                case "text":
                    return TEXT;
                case "date":
                    return DATE;
                case "time":
                    return TIME;
                case "timestamp":
                    return TIMESTAMP;
                case "tinyint":
                    return TINYINT;
                case "smallint":
                    return SMALLINT;
                case "int":
                    return INT;
                case "bigint":
                    return BIGINT;
                case "varint":
                    return VARINT;
                case "float":
                    return FLOAT;
                case "double":
                    return DOUBLE;
                case "decimal":
                    return DECIMAL;
                case "boolean":
                    return BOOLEAN;
                case "blob":
                    return BLOB;
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

    /**
     * Converts a logisland record into a list of cassandra values to insert
     * @param record
     * @param fieldsToType map of fields to use and their expected cassandra type
     * @return
     * @throws Exception
     */
    public static List<Object> convertInsert(Record record, Map<String, CassandraType> fieldsToType) throws Exception {

        List<Object> result = new ArrayList<Object>();

        for (Map.Entry<String, CassandraType> entry : fieldsToType.entrySet())
        {
            String fieldName = entry.getKey();
            Field field = record.getField(fieldName);
            if (field == null)
            {
                throw new Exception("Field " + fieldName + " does not exist in record: " + record);
            }
            CassandraType cassandraType = entry.getValue();
            result.add(convertToCassandraValue(field, cassandraType));
        }

        return result;
    }

    /**
     * Converts a logisland field value into the matching cassandra object
     * @param field Input field
     * @param cassandraType Expected cassandra type
     * @return
     * @throws Exception
     */
    private static Object convertToCassandraValue(Field field, CassandraType cassandraType) throws Exception {

        switch(cassandraType)
        {
            case UUID:
                return convertToCassandraUuidValue(field);
            case TEXT:
                return convertToCassandraTextValue(field);
            case DATE:
                return convertToCassandraDateValue(field);
            case TIME:
                return convertToCassandraTimeValue(field);
            case TIMESTAMP:
                return convertToCassandraTimestampValue(field);
            case TINYINT:
                return convertToCassandraTinyintValue(field);
            case SMALLINT:
                return convertToCassandraSmallintValue(field);
            case INT:
                return convertToCassandraIntValue(field);
            case BIGINT:
                return convertToCassandraBigintValue(field);
            case VARINT:
                return convertToCassandraVarintValue(field);
            case FLOAT:
                return convertToCassandraFloatValue(field);
            case DOUBLE:
                return convertToCassandraDoubleValue(field);
            case DECIMAL:
                return convertToCassandraDecimalValue(field);
            case BOOLEAN:
                return convertToCassandraBooleanValue(field);
            case BLOB:
                return convertToCassandraBlobValue(field);
            default:
                throw new Exception("Unsupported cassandra type " + cassandraType + " used for field " + field.getName());
        }
    }

    private static Object convertToCassandraBigintValue(Field field) {
        return field.asLong();
    }

    private static Object convertToCassandraSmallintValue(Field field) {
        return new Short(field.asInteger().toString());
    }

    private static Object convertToCassandraTinyintValue(Field field) {
        return new Byte(field.asInteger().toString());
    }

    private static Object convertToCassandraBlobValue(Field field) throws Exception {
        // We expect the type to be a byte array
        FieldType fieldType = field.getType();
        if (fieldType != FieldType.BYTES)
        {
            throw new Exception("Field type for field "  + field.getName() +
                    " should be BYTES to be converted for cassandra but it is: " + fieldType);
        }
        Object rawValue = field.getRawValue();
        byte[] bytes = (byte[])rawValue;
        return ByteBuffer.wrap(bytes); // Cassandra driver expects a ByteBuffer for blob type
    }

    private static Object convertToCassandraBooleanValue(Field field) throws Exception {
        return field.asBoolean();
    }

    private static Object convertToCassandraVarintValue(Field field) throws Exception {
        return new BigInteger(field.asLong().toString());
    }

    private static Object convertToCassandraDecimalValue(Field field) throws Exception {
        return new BigDecimal(field.asDouble());
    }

    private static Object convertToCassandraDoubleValue(Field field) throws Exception {
        return field.asDouble();
    }

    private static Object convertToCassandraFloatValue(Field field) throws Exception {
        return field.asFloat();
    }

    private static Object convertToCassandraTimestampValue(Field field) throws Exception {
        // Timestamps may be expressed either in integer or string form:
        // see http://cassandra.apache.org/doc/latest/cql/types.html#timestamps
        FieldType fieldType = field.getType();
        if (fieldType == FieldType.STRING)
        {
            /**
             * string that represents an ISO 8601 date. For instance, all of the values below are valid timestamp values for Mar 2, 2011, at 04:05:00 AM, GMT:
             *
             *     1299038700000
             *     '2011-02-03 04:05+0000'
             *     '2011-02-03 04:05:00+0000'
             *     '2011-02-03 04:05:00.000+0000'
             *     '2011-02-03T04:05+0000'
             *     '2011-02-03T04:05:00+0000'
             *     '2011-02-03T04:05:00.000+0000'
             */
            return field.asString();
        } else
        {
            // type are encoded as 64-bit signed integers representing a number of milliseconds since the standard base
            // time known as the epoch: January 1 1970 at 00:00:00 GMT.
            return new Date(field.asLong());
        }
    }

    private static Object convertToCassandraTimeValue(Field field) throws Exception {
        // Times may be expressed either in integer or string form:
        // see http://cassandra.apache.org/doc/latest/cql/types.html#times
        FieldType fieldType = field.getType();
        if (fieldType == FieldType.STRING)
        {
            /**
             *  the format should be hh:mm:ss[.fffffffff] (where the sub-second precision is optional and if provided, can be less than the nanosecond). So for instance, the following are valid inputs for a time:
             *
             *     '08:12:54'
             *     '08:12:54.123'
             *     '08:12:54.123456'
             *     '08:12:54.123456789'
             */
            return field.asString();
        } else
        {
            // type are encoded as 64-bit signed integers representing the number of nanoseconds since midnight.
            return field.asLong();
        }
    }

    private static Object convertToCassandraDateValue(Field field) throws Exception {
        // Dates may be expressed either in integer or string form:
        // see http://cassandra.apache.org/doc/latest/cql/types.html#dates
        FieldType fieldType = field.getType();
        if (fieldType == FieldType.STRING)
        {
            return field.asString(); // the format should be yyyy-mm-dd (so '2011-02-03' for instance).
        } else
        {
            return LocalDate.fromDaysSinceEpoch(field.asLong().intValue()); // date type are encoded as 32-bit unsigned integers representing a number of days with “the epoch”
        }
    }

    private static Object convertToCassandraTextValue(Field field) throws Exception {
        return field.asString();
    }

    private static Object convertToCassandraUuidValue(Field field) throws Exception {
        return UUID.fromString(field.asString());
    }

    private static Object convertToCassandraIntValue(Field field) throws Exception {
        return field.asInteger();
    }
}
