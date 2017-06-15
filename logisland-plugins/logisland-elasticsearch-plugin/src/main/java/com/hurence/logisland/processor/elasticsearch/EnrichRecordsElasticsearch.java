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
package com.hurence.logisland.processor.elasticsearch;


import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Tags({"elasticsearch"})
@CapabilityDescription("Enrich input records with content indexed in elasticsearch using multiget queries.\n" +
        "Each incoming record must be possibly enriched with information stored in elasticsearch. \n"+
        "The plugin properties are :\n" +
        "- es.index (String)            : Name of the elasticsearch index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        "- record.key (String)          : Name of the field in the input record containing the id to lookup document in elastic search. This field is mandatory.\n" +
        "- es.key (String)              : Name of the elasticsearch key on which the multiget query will be performed. This field is mandatory.\n" +
        "- includes (ArrayList<String>) : List of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "- excludes (ArrayList<String>) : List of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "\n" +
        "Each outcoming record holds at least the input record plus potentially one or more fields coming from of one elasticsearch document."
)
public class EnrichRecordsElasticsearch extends AbstractElasticsearchProcessor
{
    public static final PropertyDescriptor RECORD_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("record.key")
            .description("The name of field in the input record containing the document id to use in ES multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index")
            .description("The name of the ES index to use in multiget query. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type")
            .description("The name of the ES type to use in multiget query. ")
            .required(false)
            .build();

    public static final PropertyDescriptor ES_INCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.includes.field")
            .description("The name of the ES fields to include in the record.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("*")
            .build();

    public static final PropertyDescriptor ES_EXCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.excludes.field")
            .description("The name of the ES fields to exclude.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("N/A")
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ELASTICSEARCH_CLIENT_SERVICE);
        props.add(RECORD_KEY_FIELD);
        props.add(ES_INDEX_FIELD);
        props.add(ES_TYPE_FIELD);
        props.add(ES_INCLUDES_FIELD);
        props.add(ES_EXCLUDES_FIELD);

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

        List<Record> outputRecords   = new ArrayList<>();
        List<Record> recordsToEnrich = new ArrayList<>();

        if (records.size() != 0) {

            String recordKeyName     = context.getPropertyValue(RECORD_KEY_FIELD).asString();
            String indexName         = context.getPropertyValue(ES_INDEX_FIELD).asString();
            String typeName         = context.getPropertyValue(ES_TYPE_FIELD).asString();
            String includesFieldName = context.getPropertyValue(ES_INCLUDES_FIELD).asString();
            String excludesFieldName = context.getPropertyValue(ES_EXCLUDES_FIELD).asString();

            List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
            List<MultiGetResponseRecord> multiGetResponseRecords;
            HashSet<String> ids = new HashSet<>(); // Use a Set to avoid duplicates

            for (Record record : records) {
                if(!record.hasField(recordKeyName) ||
                        record.getField(recordKeyName) == null ||
                        record.getField(recordKeyName).getRawValue() == null ||
                        record.getField(recordKeyName).asString().isEmpty() ) {
                    // The record will not be enriched
                    // Register the record and process the next one
                    outputRecords.add(record);
                    continue;
                }
                else {
                    // The record will potentially be enriched
                    ids.add(record.getField(recordKeyName).asString());
                    recordsToEnrich.add(record);
                }
            }

            if (ids.isEmpty()){
                return records;
            }

            // Includes :
            String[] includesArray = null;
            if((includesFieldName != null) && (! includesFieldName.isEmpty())) {
                includesArray = includesFieldName.split("\\s*,\\s*");
            }

            // Excludes :
            String[] excludesArray = null;
            if((excludesFieldName != null) && (! excludesFieldName.isEmpty())) {
                excludesArray = excludesFieldName.split("\\s*,\\s*");
            }

            try {
                multiGetQueryRecords.add(new MultiGetQueryRecord(indexName, typeName, new ArrayList(ids), includesArray, excludesArray));
            } catch (InvalidMultiGetQueryRecordException e) {
                // Cannot enrich any records
                // Return input records
                return records;
            }

            multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

            if (multiGetResponseRecords == null || multiGetResponseRecords.isEmpty()){
                return records;
            }

            // Transform the returned documents from ES in a Map
            Map<String, MultiGetResponseRecord> responses = multiGetResponseRecords.
                    stream().
                    collect(Collectors.toMap(MultiGetResponseRecord::getDocumentId, Function.identity()));

            recordsToEnrich.forEach(recordToEnrich -> {
                Record outputRecord = recordToEnrich;
                MultiGetResponseRecord responseRecord = responses.get(outputRecord.getField(recordKeyName).asString());
                if((responseRecord != null) && (responseRecord.getRetrievedFields() != null))
                {
                    responseRecord.getRetrievedFields().forEach((k,v) -> {
                        outputRecord.setStringField(k.toString(), v.toString());
                    });
                }
                outputRecords.add(outputRecord);
            });
        }
        return outputRecords;
    }
}
