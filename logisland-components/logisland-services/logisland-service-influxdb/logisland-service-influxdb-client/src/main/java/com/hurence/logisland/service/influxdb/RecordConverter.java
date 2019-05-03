/**
 * Copyright (C) 2019 Hurence (support@hurence.com)
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
package com.hurence.logisland.service.influxdb;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.FieldDictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hurence.logisland.service.influxdb.InfluxDBUpdater.DataToInsert;
import com.hurence.logisland.service.influxdb.InfluxDBUpdater.InfluxDBField;
import com.hurence.logisland.service.influxdb.InfluxDBUpdater.InfluxDBType;
import com.hurence.logisland.service.influxdb.InfluxDBControllerService.CONFIG_MODE;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class converts a logisland Record to some InfluxDB values to insert
 */
public class RecordConverter {

    private static Logger logger = LoggerFactory.getLogger(RecordConverter.class.getName());

    /**
     * Converts a logisland record into a easily usable data to insert object. Determining the values for fields and tags
     * @param record The logisland record to transform
     * @param measurement The measurement name
     * @param mode The configuration mode
     * @param tags The list of tags (may be null or not, depending on the configuration mode)
     * @param fields The list of fields (may be null or not, depending on the configuration mode)
     * @param timeField The time field and his format in a list (may be null or not, depending on the configuration)
     * @return A usable object to insert data into InfluxDB
     * @throws Exception
     */
    public static DataToInsert convertToInfluxDB(Record record, String measurement, CONFIG_MODE mode,
                                             Set<String> tags, Set<String> fields, List<Object> timeField) throws Exception {

        Map<String, String> lowerCaseToUpperCaseFields = null;
        DataToInsert dataToInsert = new DataToInsert();
        dataToInsert.measurement = measurement;

        Set<String> tagFields = new HashSet<String>();
        Set<String> fieldFields = new HashSet<String>();
        Set<String> allFields = filterLogislandFields(record.getAllFieldNames());

        // Remove time field if any configured
        String timeFieldName = null;
        TimeUnit timeFieldFormat = null;
        if (timeField != null)
        {
            timeFieldName = (String)timeField.get(0);
            timeFieldFormat = (TimeUnit)timeField.get(1);
            allFields.remove(timeFieldName);
        }

        // Fill the tags and fields list according to the configuration mode
        switch (mode)
        {
            case EXPLICIT_TAGS_AND_FIELDS:
                tagFields = (tags != null ? tags : new HashSet<String>());
                fieldFields = (fields != null ? fields : new HashSet<String>());
                break;
            case ALL_AS_FIELDS:
                fieldFields = allFields;
                break;
            case ALL_AS_TAGS_BUT_EXPLICIT_FIELDS:
                fieldFields = (fields != null ? fields : new HashSet<String>());
                tagFields = new HashSet<String>();
                for (String field : allFields)
                {
                    if (!fieldFields.contains(field))
                    {
                        tagFields.add(field);
                    }
                }
                break;
            case ALL_AS_FIELDS_BUT_EXPLICIT_TAGS:
                tagFields = (tags != null ? tags : new HashSet<String>());
                fieldFields = new HashSet<String>();
                for (String field : allFields)
                {
                    if (!tagFields.contains(field))
                    {
                        fieldFields.add(field);
                    }
                }
                break;
        }

        // Add tag values
        for (String field : tagFields)
        {
            String value = logislandFieldToInfluxDBTagValue(record.getField(field));
            if (value != null)
            {
                dataToInsert.tags.put(field, value);
            }
        }

        // Add field values
        for (String field : fieldFields)
        {
            InfluxDBField influxDBField = logislandFieldToInfluxDBField(record.getField(field));
            if (influxDBField != null)
            {
                dataToInsert.fields.put(field, influxDBField);
            }
        }

        // Add point time
        if (timeFieldName != null)
        {
            // Use defined time field
            Field recordTimeField = record.getField(timeFieldName);
            if (recordTimeField == null)
            {
                throw new Exception("Measurement " + measurement + ": defined time field " + timeFieldName + " but no" +
                        " such field in record (ignoring record): " + record);
            }
            dataToInsert.time = toInfluxDBTime(recordTimeField, timeFieldFormat);
            dataToInsert.format = timeFieldFormat;
        } else
        {
            // Use record_time field
            Field recordTimeField = record.getField(FieldDictionary.RECORD_TIME);
            if (recordTimeField == null)
            {
                throw new Exception("Measurement " + measurement + ": no time field defined and no " +
                        FieldDictionary.RECORD_TIME + " field in record (ignoring record): " + record);
            }
            dataToInsert.time = recordTimeField.asLong();
            dataToInsert.format = TimeUnit.MILLISECONDS;
        }

        return dataToInsert;
    }

    /**
     * Convert time field value to a usable influx db time value
     * @param field
     * @param format
     * @return
     */
    private static long toInfluxDBTime(Field field, TimeUnit format) {

        switch (field.getType())
        {
            case INT:
                return field.asInteger();
            case LONG:
                return field.asLong();
            case FLOAT:
                return (long)field.asFloat().floatValue();
            case DOUBLE:
                return (long)field.asDouble().doubleValue();
            case STRING:
                String strValue = field.asString();
                if (strValue.length() == 0)
                {
                    // Should not happen
                    logger.error("Empty time field: " + field);
                    return 0L;
                } else
                {
                    try {
                        return Long.parseLong(strValue);
                    } catch(Throwable t)
                    {
                        // Should not happen
                        logger.error("Cannot get a long from time field: " + field);
                        return 0L;
                    }
                }
            case BOOLEAN:
            case ENUM:
            case DATETIME:
            case ARRAY:
            case RECORD:
            case MAP:
            case UNION:
            case BYTES:
            case NULL:
                // Should not happen
                logger.error("Unsupported time field: " + field);
                return 0L;
            default:
                // Should not happen
                logger.error("How did we get there?: " + field);
                return 0L;
        }
    }

    /**
     * Filter logisland technical fields
     * @param originalFields
     * @return
     */
    private static Set<String> filterLogislandFields(Set<String> originalFields) {

        Set<String> finalFields = new HashSet<String>();

        originalFields.forEach(field -> {

            if (!FieldDictionary.TECHNICAL_FIELDS.contains(field))
            {
                finalFields.add(field);
            }
        });

        return finalFields;
    }

    /**
     * Converts a logisland record field to an InfluxDB tag value
     * @param field The field to convert
     * @return The converted value
     */
    private static String logislandFieldToInfluxDBTagValue(Field field) {

        if (field == null)
        {
            return null;
        }

        return field.getRawValue().toString();
    }

    /**
     * Converts a logisland record field to an InfluxDB field value
     * @param field The field to convert
     * @return The converted value
     */
    private static InfluxDBField logislandFieldToInfluxDBField(Field field) {

        if (field == null)
        {
            return null;
        }

        switch (field.getType())
        {
            case INT:
            case LONG:
                return new InfluxDBField(InfluxDBType.INTEGER, field.asLong());
            case FLOAT:
            case DOUBLE:
                return new InfluxDBField(InfluxDBType.FLOAT, field.asDouble());
            case BOOLEAN:
                return new InfluxDBField(InfluxDBType.BOOLEAN, field.asBoolean());
            case STRING:
                String strValue = field.asString();
                if (strValue.length() == 0)
                {
                    return null;
                } else
                {
                    return new InfluxDBField(InfluxDBType.STRING, strValue);
                }
            case ENUM:
            case DATETIME:
                return new InfluxDBField(InfluxDBType.STRING, field.getRawValue().toString());
            case ARRAY:
            case RECORD:
            case MAP:
            case UNION:
            case BYTES:
                String objectValue = field.getRawValue().toString();
                if (objectValue.isEmpty())
                {
                    return null;
                } else
                {
                    return new InfluxDBField(InfluxDBType.STRING, objectValue);
                }
            case NULL:
                return null;
            default:
                return null;
        }
    }
}
