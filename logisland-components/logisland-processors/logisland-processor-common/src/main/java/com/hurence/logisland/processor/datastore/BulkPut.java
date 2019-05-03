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
package com.hurence.logisland.processor.datastore;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.ExtraDetailFile;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.AllowableValue;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.validator.StandardValidators;
import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"datastore", "record", "put", "bulk"})
@CapabilityDescription("Indexes the content of a Record in a Datastore using bulk processor")
@ExtraDetailFile("./details/common-processors/BulkPut-Detail.rst")
public class BulkPut extends AbstractDatastoreProcessor
{

    public static final PropertyDescriptor DEFAULT_COLLECTION = new PropertyDescriptor.Builder()
            .name("default.collection")
            .description("The name of the collection/index/table to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .build();


    public static final PropertyDescriptor COLLECTION_FIELD = new PropertyDescriptor.Builder()
            .name("collection.field")
            .description("the name of the event field containing es index name => will override index value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DEFAULT_TYPE = new PropertyDescriptor.Builder()
            .name("default.type")
            .description("The type of this document (required by Elasticsearch for indexing and searching)")
            .required(false)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("type.field")
            .description("the name of the event field containing es doc type => will override type value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final AllowableValue NO_DATE_SUFFIX = new AllowableValue("no", "No date",
            "no date added to default index");

    public static final AllowableValue TODAY_DATE_SUFFIX = new AllowableValue("today", "Today's date",
            "today's date added to default index");

    public static final AllowableValue YESTERDAY_DATE_SUFFIX = new AllowableValue("yesterday", "yesterday's date",
            "yesterday's date added to default index");

    public static final PropertyDescriptor TIMEBASED_INDEX = new PropertyDescriptor.Builder()
            .name("timebased.collection")
            .description("do we add a date suffix")
            .required(true)
            .allowableValues(NO_DATE_SUFFIX, TODAY_DATE_SUFFIX, YESTERDAY_DATE_SUFFIX)
            .defaultValue(NO_DATE_SUFFIX.getValue())
            .build();

    public static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("date.format")
            .description("simple date format for date suffix. default : yyyy.MM.dd")
            .required(false)
            .defaultValue("yyyy.MM.dd")
            .build();



    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATASTORE_CLIENT_SERVICE);
        props.add(DEFAULT_COLLECTION);
        props.add(DEFAULT_TYPE);
        props.add(TIMEBASED_INDEX);
        props.add(DATE_FORMAT);
        props.add(COLLECTION_FIELD);
        props.add(TYPE_FIELD);

        return Collections.unmodifiableList(props);
    }

    /**
     * process events
     *
     * @param context
     * @param records
     * @return
     */
    @Override
    public Collection<Record> process(final ProcessContext context, final Collection<Record> records) {

        // check if we need initialization
        if(datastoreClientService == null) {
            init(context);
        }

        // bail out if init has failed
        if(datastoreClientService == null) {
            return records;
        }

        if (records.size() != 0) {

            /**
             * compute global index from Processor settings
             */
            String defaultCollection = context.getPropertyValue(DEFAULT_COLLECTION).asString();
            if (context.getPropertyValue(TIMEBASED_INDEX).isSet()) {
                final SimpleDateFormat sdf = new SimpleDateFormat(context.getPropertyValue(DATE_FORMAT).asString());
                if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(TODAY_DATE_SUFFIX.getValue())) {
                    defaultCollection += "." + sdf.format(new Date());
                } else if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(YESTERDAY_DATE_SUFFIX.getValue())) {
                    DateTime dt = new DateTime(new Date()).minusDays(1);
                    defaultCollection += "." + sdf.format(dt.toDate());
                }
            }

            /**
             * loop over events to add them to bulk
             */
            for (Record record : records) {
                String collection = defaultCollection;
                if (context.getPropertyValue(COLLECTION_FIELD).isSet()) {
                    Field eventIndexField = record.getField(context.getPropertyValue(COLLECTION_FIELD).asString());
                    if (eventIndexField != null && eventIndexField.getRawValue() != null) {
                        collection = eventIndexField.getRawValue().toString();
                    }
                }

                if (context.getPropertyValue(DEFAULT_TYPE).isSet()) {
                    // compute type from event if any
                    String docType = context.getPropertyValue(DEFAULT_TYPE).asString();
                    if (context.getPropertyValue(TYPE_FIELD).isSet()) {
                        Field eventTypeField = record.getField(context.getPropertyValue(TYPE_FIELD).asString());
                        if (eventTypeField != null && eventTypeField.getRawValue() != null) {
                            docType = eventTypeField.getRawValue().toString();
                        }
                    }
                    datastoreClientService.bulkPut(collection.concat(","+docType), record);
                }
                else {
                    datastoreClientService.bulkPut(collection, record);
                }
            }

            datastoreClientService.bulkFlush();
        }
        return records;
    }
}