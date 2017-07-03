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
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.service.elasticsearch.multiGet.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetQueryRecord;
import com.hurence.logisland.service.elasticsearch.multiGet.MultiGetResponseRecord;
import com.hurence.logisland.record.*;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;

@Tags({"elasticsearch"})
@CapabilityDescription("Retrieves a content indexed in elasticsearch using elasticsearch multiget queries.\n" +
        "Each incoming record contains information regarding the elasticsearch multiget query that will be performed. This information is stored in record fields whose names are configured in the plugin properties (see below) :\n" +
        "- index (String) : name of the elasticsearch index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        "- type (String) : name of the elasticsearch type on which the multiget query will be performed. This field is not mandatory.\n" +
        "- ids (String) : comma separated list of document ids to fetch. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        "- includes (String) : comma separated list of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "- excludes (String) : comma separated list of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "\n" +
        "Each outcoming record holds data of one elasticsearch retrieved document. This data is stored in these fields :\n" +
        "- index (same field name as the incoming record) : name of the elasticsearch index.\n" +
        "- type (same field name as the incoming record) : name of the elasticsearch type.\n" +
        "- id (same field name as the incoming record) : retrieved document id.\n" +
        "- a list of String fields containing :\n" +
        "   * field name : the retrieved field name\n" +
        "   * field value : the retrieved field value"
)
public class MultiGetElasticsearch extends AbstractElasticsearchProcessor
{

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index.field")
            .description("the name of the incoming records field containing es index name to use in multiget query. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type.field")
            .description("the name of the incoming records field containing es type name to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_IDS_FIELD = new PropertyDescriptor.Builder()
            .name("es.ids.field")
            .description("the name of the incoming records field containing es document Ids to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_INCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.includes.field")
            .description("the name of the incoming records field containing es includes to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ES_EXCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.excludes.field")
            .description("the name of the incoming records field containing es excludes to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ELASTICSEARCH_CLIENT_SERVICE);
        props.add(ES_INDEX_FIELD);
        props.add(ES_TYPE_FIELD);
        props.add(ES_IDS_FIELD);
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

        List<Record> outputRecords = new ArrayList<>();

        if (records.size() != 0) {

            String indexFieldName = context.getPropertyValue(ES_INDEX_FIELD).asString();
            String typeFieldName = context.getPropertyValue(ES_TYPE_FIELD).asString();
            String idsFieldName = context.getPropertyValue(ES_IDS_FIELD).asString();
            String includesFieldName = context.getPropertyValue(ES_INCLUDES_FIELD).asString();
            String excludesFieldName = context.getPropertyValue(ES_EXCLUDES_FIELD).asString();

            List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();

            for (Record record : records) {

                if(     !record.hasField(indexFieldName)  // record doesn't contain index field
                        || record.getField(indexFieldName) == null // index field is null
                        || !record.getField(indexFieldName).getType().equals(FieldType.STRING) // index field is not of STRING type
                        || record.getField(indexFieldName).getRawValue() == null // index field raw value is null
                        || record.getField(indexFieldName).getRawValue().toString().isEmpty() // index field is empty
                        ) {
                    StandardRecord outputRecord = new StandardRecord(record);
                    outputRecord.addError(ProcessError.BAD_RECORD.getName(), "record must have the field "
                            + indexFieldName + " containing the index name to use in the multiget query. This field must be of STRING type and cannot be empty.");
                    outputRecords.add(outputRecord);
                    continue;
                }

                if(     !record.hasField(idsFieldName)
                        || record.getField(idsFieldName) == null
                        || !record.getField(idsFieldName).getType().equals(FieldType.STRING)
                        || record.getField(idsFieldName).getRawValue() == null
                        || record.getField(idsFieldName).getRawValue().toString().isEmpty() ) {
                    StandardRecord outputRecord = new StandardRecord(record);
                    outputRecord.addError(ProcessError.BAD_RECORD.getName(), "record must have the field " + idsFieldName + " containing the Ids to use in the multiget query. This field must be of STRING type and cannot be empty.");
                    outputRecords.add(outputRecord);
                    continue;
                }

                // Index :
                String index = record.getField(indexFieldName).asString();

                // Type :
                String type = ( record.getField(typeFieldName) != null
                        && record.getField(typeFieldName).asString().trim().length() > 0 )  // if the type is empty (whitespaces), consider it is not set (type = null)
                        ? record.getField(typeFieldName).asString()
                        : null;

                // Document Ids :
                String idsString = record.getField(idsFieldName).asString();
                List<String> idsList = new ArrayList<>(Arrays.asList(idsString.split("\\s*,\\s*")));

                // Includes :
                String[] includesArray;
                if(record.getField(includesFieldName) != null && record.getField(includesFieldName).getRawValue() != null) {
                    String includesString = record.getField(includesFieldName).asString();
                    List<String> includesList = new ArrayList<>(Arrays.asList(includesString.split("\\s*,\\s*")));
                    includesArray = new String[includesList.size()];
                    includesArray = includesList.toArray(includesArray);
                } else {
                    includesArray = null;
                }

                // Excludes :
                String[] excludesArray;
                if(record.getField(excludesFieldName) != null && record.getField(excludesFieldName).getRawValue() != null) {
                    String excludesString = record.getField(excludesFieldName).asString();
                    List<String> excludesList = new ArrayList<>(Arrays.asList(excludesString.split("\\s*,\\s*")));
                    excludesArray = new String[excludesList.size()];
                    excludesArray = excludesList.toArray(excludesArray);
                } else {
                    excludesArray = null;
                }

                try {
                    multiGetQueryRecords.add(new MultiGetQueryRecord(index, type, includesArray, excludesArray, idsList));
                } catch (InvalidMultiGetQueryRecordException e) {
                    StandardRecord outputRecord = new StandardRecord(record);
                    outputRecord.addError(ProcessError.BAD_RECORD.getName(), e.getMessage());
                    outputRecords.add(outputRecord);
                    continue;
                }
            }

            List<MultiGetResponseRecord> multiGetResponseRecords = elasticsearchClientService.multiGet(multiGetQueryRecords);

            multiGetResponseRecords.forEach(responseRecord -> {
                StandardRecord outputRecord = new StandardRecord();
                outputRecord.setStringField(indexFieldName, responseRecord.getIndexName());
                outputRecord.setStringField(typeFieldName, responseRecord.getTypeName());
                outputRecord.setStringField(idsFieldName, responseRecord.getDocumentId());
                if(responseRecord.getRetrievedFields() != null)
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
