package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
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

@Tags({"elasticsearch"})
@CapabilityDescription("Indexes the content of a Record in Elasticsearch using elasticsearch's bulk processor")
public class BulkAddElasticsearch extends AbstractElasticsearchProcessor
{

    public static final PropertyDescriptor DEFAULT_INDEX = new PropertyDescriptor.Builder()
            .name("default.index")
            .description("The name of the index to insert into")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DEFAULT_TYPE = new PropertyDescriptor.Builder()
            .name("default.type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final AllowableValue NO_DATE_SUFFIX = new AllowableValue("no", "No date",
            "no date added to default index");

    public static final AllowableValue TODAY_DATE_SUFFIX = new AllowableValue("today", "Today's date",
            "today's date added to default index");

    public static final AllowableValue YESTERDAY_DATE_SUFFIX = new AllowableValue("yesterday", "yesterday's date",
            "yesterday's date added to default index");

    public static final PropertyDescriptor TIMEBASED_INDEX = new PropertyDescriptor.Builder()
            .name("timebased.index")
            .description("do we add a date suffix")
            .required(true)
            .allowableValues(NO_DATE_SUFFIX, TODAY_DATE_SUFFIX, YESTERDAY_DATE_SUFFIX)
            .defaultValue(NO_DATE_SUFFIX.getValue())
            .build();

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index.field")
            .description("the name of the event field containing es index name => will override index value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type.field")
            .description("the name of the event field containing es doc type => will override type value if set")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ELASTICSEARCH_CLIENT_SERVICE);
        props.add(DEFAULT_INDEX);
        props.add(DEFAULT_TYPE);
        props.add(TIMEBASED_INDEX);
        props.add(ES_INDEX_FIELD);
        props.add(ES_TYPE_FIELD);

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
        if (records.size() != 0) {

            /**
             * compute global index from Processor settings
             */
            String defaultIndex = context.getPropertyValue(DEFAULT_INDEX).asString();
            String defaultType = context.getPropertyValue(DEFAULT_TYPE).asString();
            if (context.getPropertyValue(TIMEBASED_INDEX).isSet()) {
                final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
                if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(TODAY_DATE_SUFFIX.getValue())) {
                    defaultIndex += "." + sdf.format(new Date());
                } else if (context.getPropertyValue(TIMEBASED_INDEX).getRawValue().equals(YESTERDAY_DATE_SUFFIX.getValue())) {
                    DateTime dt = new DateTime(new Date()).minusDays(1);
                    defaultIndex += "." + sdf.format(dt.toDate());
                }
            }

            /**
             * loop over events to add them to bulk
             */
            for (Record record : records) {

                // compute es index from event if any
                String docIndex = defaultIndex;
                if (context.getPropertyValue(ES_INDEX_FIELD).isSet()) {
                    Field eventIndexField = record.getField(context.getPropertyValue(ES_INDEX_FIELD).asString());
                    if (eventIndexField != null && eventIndexField.getRawValue() != null) {
                        docIndex = eventIndexField.getRawValue().toString();
                    }
                }

                // compute es type from event if any
                String docType = defaultType;
                if (context.getPropertyValue(ES_TYPE_FIELD).isSet()) {
                    Field eventTypeField = record.getField(context.getPropertyValue(ES_TYPE_FIELD).asString());
                    if (eventTypeField != null && eventTypeField.getRawValue() != null) {
                        docType = eventTypeField.getRawValue().toString();
                    }
                }

                String document = elasticsearchClientService.convertRecordToString(record);
                elasticsearchClientService.bulkPut(docIndex, docType, document, Optional.of(record.getId()));

            }
        }
        return records;
    }
}
