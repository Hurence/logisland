/**
 * Copyright (C) 2017 Hurence (bailet.thomas@gmail.com)
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

package com.hurence.logisland.processor.elasticsearch;

import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.TimeZone;

public class ElasticsearchRecordConverter {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchRecordConverter.class);

    /**
     * Converts an Event into an Elasticsearch document
     * to be indexed later
     *
     * @param record
     * @return
     */
    public static String convertToString(Record record) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String document = "";

            // convert event_time as ISO for ES
            if (record.hasField(FieldDictionary.RECORD_TIME)) {
                try {
                    DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeNoMillis();
                    document += "@timestamp : ";
                    document += dateParser.print(record.getField(FieldDictionary.RECORD_TIME).asLong()) + ", ";
                } catch (Exception ex) {
                    logger.error("unable to parse record_time iso date for {}", record);
                }
            }

            // add all other records
            for (Iterator<Field> i = record.getAllFieldsSorted().iterator(); i.hasNext();) {
                Field field = i.next();
                String fieldName = field.getName().replaceAll("\\.", "_");

                switch (field.getType()) {

                    case STRING:
                        document += fieldName + " : " + field.asString();
                        break;
                    case INT:
                        document += fieldName + " : " + field.asInteger().toString();
                        break;
                    case LONG:
                        document += fieldName + " : " + field.asLong().toString();
                        break;
                    case FLOAT:
                        document += fieldName + " : " + field.asFloat().toString();
                        break;
                    case DOUBLE:
                        document += fieldName + " : " + field.asDouble().toString();
                        break;
                    case BOOLEAN:
                        document += fieldName + " : " + field.asBoolean().toString();
                        break;
                    default:
                        document += fieldName + " : " + field.getRawValue().toString();
                        break;
                }
            }

            return document;
        } catch (Throwable ex) {
            logger.error("unable to convert record : {}, {}", record, ex.toString());
        }
        return null;
    }

}