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

package com.hurence.logisland.service.elasticsearchasaservice;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
            XContentBuilder document = jsonBuilder().startObject();

            // convert event_time as ISO for ES
            if (record.hasField(FieldDictionary.RECORD_TIME)) {
                try {
                    DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeNoMillis();
                    document.field("@timestamp", dateParser.print(record.getField(FieldDictionary.RECORD_TIME).asLong()));
                } catch (Exception ex) {
                    logger.error("unable to parse record_time iso date for {}", record);
                }
            }

            // add all other records
            record.getAllFieldsSorted().forEach(field -> {
                try {
                    // cleanup invalid es fields characters like '.'
                    String fieldName = field.getName().replaceAll("\\.", "_");

                    switch (field.getType()) {

                        case STRING:
                            document.field(fieldName, field.asString());
                            break;
                        case INT:
                            document.field(fieldName, field.asInteger().intValue());
                            break;
                        case LONG:
                            document.field(fieldName, field.asLong().longValue());
                            break;
                        case FLOAT:
                            document.field(fieldName, field.asFloat().floatValue());
                            break;
                        case DOUBLE:
                            document.field(fieldName, field.asDouble().doubleValue());
                            break;
                        case BOOLEAN:
                            document.field(fieldName, field.asBoolean().booleanValue());
                            break;
                        default:
                            document.field(fieldName, field.getRawValue());
                            break;
                    }

                } catch (Throwable ex) {
                    logger.error("unable to process a field in record : {}, {}", record, ex.toString());
                }
            });

            String result = document.endObject().string();
            document.flush();
            return result;
        } catch (Throwable ex) {
            logger.error("unable to convert record : {}, {}", record, ex.toString());
        }
        return null;
    }

}