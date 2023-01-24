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
package com.hurence.logisland.service.elasticsearch;

import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.RecordUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

class ElasticsearchRecordConverter {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchRecordConverter.class);

    /**
     * Converts an Event into an Elasticsearch document
     * to be indexed later
     *e
     * @param record to convert
     * @return the json converted record
     */
    static String convertToString(Record record) {
      return convertToString(record, "location");
    }


    /**
     * Converts an Event into an Elasticsearch document
     * to be indexed later
     *e
     * @param record to convert
     * @param geolocationFieldLabel is the label for the geolocation field
     * @return the json converted record
     */
    static String convertToString(Record record, String geolocationFieldLabel) {
        logger.trace(record.toString());
        try {
            final XContentBuilder document = jsonBuilder().startObject();
            // convert event_time as ISO for ES
            if (record.hasField(FieldDictionary.RECORD_TIME)) {
                try {
                    DateTimeFormatter dateParser = ISODateTimeFormat.dateTimeNoMillis();
                    document.field("@timestamp", dateParser.print(record.getField(FieldDictionary.RECORD_TIME).asLong()));
                } catch (Exception ex) {
                    logger.error("unable to parse record_time iso date for {}", record);
                }
            }

            final float[] geolocation = new float[2];
            // add all other records
            record.getAllFieldsSorted().forEach(field -> {

                if (field.getName().equals("@timestamp"))
                {
                    // #489
                    // Ignore potentially already existing @timestamp field in the record.
                    // We have already written it here above with the record_time value in the document and both fields
                    // must be aligned. So forget about an already existing value.
                    return;
                }

                final String fieldName = field.getName().replaceAll("\\.", "_");
                try {
                    // cleanup invalid es fields characters like '.'
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
                            if( fieldName.equals("lat") || fieldName.equals("latitude"))
                                geolocation[0] = field.asFloat();
                            if( fieldName.equals("long") || fieldName.equals("longitude"))
                                geolocation[1] = field.asFloat();

                            break;
                        case DOUBLE:
                            document.field(fieldName, field.asDouble().doubleValue());
                            if( fieldName.equals("lat") || fieldName.equals("latitude"))
                                geolocation[0] = field.asFloat();
                            if( fieldName.equals("long") || fieldName.equals("longitude"))
                                geolocation[1] = field.asFloat();
                            break;
                        case BOOLEAN:
                            document.field(fieldName, field.asBoolean().booleanValue());
                            break;
                        case RECORD:
                            Map<String, Object> map =  RecordUtils.toMap(field.asRecord(), true);
                            document.field(fieldName, map);
                            break;
                        case ARRAY:
                            Collection collection;
                            Object value = field.getRawValue();
                            if (value == null) {
                                break;
                            } else if (value.getClass().isArray()) {
                                collection = new ArrayList<>();
                                for (int i = 0; i < Array.getLength(value); i++) {
                                    collection.add(Array.get(value, i));
                                }
                            } else if (value instanceof Collection) {
                                collection = (Collection) value;
                            } else {
                                logger.error("unable to process field '{}' in record : {}, {}", fieldName, record, "the value of field is not an array or collection");
                                break;
                            }

                            final List list = new ArrayList(collection.size());
                            for (final Object item : collection) {
                                if (item instanceof Record) {
                                    list.add(RecordUtils.toMap((Record) item, true));
                                } else {
                                    list.add(item);
                                }
                            }
                            document.field(fieldName, list);
                            break;
                        default:
                            document.field(fieldName, field.getRawValue());
                            break;
                    }
                } catch (Throwable ex) {
                    logger.error("unable to process field '{}' in record : {}, {}", fieldName, record, ex.toString());
                    if ( logger.isDebugEnabled() ) {
                        ex.printStackTrace();
                    }
                }
            });


            if((geolocation[0] != 0) && (geolocation[1] != 0)) {
                document.latlon(geolocationFieldLabel, geolocation[0], geolocation[1]);
            }


            String result = Strings.toString(document.endObject());
            document.flush();
            logger.trace(result);
            return result;
        } catch (Throwable ex) {
            logger.error("unable to convert record : {}, {}", record, ex.toString());
        }
        return null;
    }

    /**
     * Returns the conversion of a record to a map where all {@code null} values were removed.
     *
     * @param record the record to convert.
     *
     * @return the conversion of a record to a map where all {@code null} values were removed.
     */
    private static Map<String, Object> toMap(final Record record) {
        return RecordUtils.toMap(record, false);
    }
}
