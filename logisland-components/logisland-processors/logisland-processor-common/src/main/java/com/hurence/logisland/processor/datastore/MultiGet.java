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


import com.hurence.logisland.annotation.documentation.*;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.processor.ProcessContext;
import com.hurence.logisland.processor.ProcessError;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.model.exception.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.datastore.model.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.model.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;

import java.util.*;


@Category(ComponentCategory.DATASTORE)
@Tags({"datastore","get", "multiget"})
@CapabilityDescription("Retrieves a content from datastore using datastore multiget queries.\n" +
        "Each incoming record contains information regarding the datastore multiget query that will be performed. This information is stored in record fields whose names are configured in the plugin properties (see below) :\n" +
        "\n" +
        " - collection (String) : name of the datastore collection on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        " - type (String) : name of the datastore type on which the multiget query will be performed. This field is not mandatory.\n" +
        " - ids (String) : comma separated list of document ids to fetch. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        " - includes (String) : comma separated list of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        " - excludes (String) : comma separated list of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "\n" +
        "Each outcoming record holds data of one datastore retrieved document. This data is stored in these fields :\n" +
        "\n" +
        " - collection (same field name as the incoming record) : name of the datastore collection.\n" +
        " - type (same field name as the incoming record) : name of the datastore type.\n" +
        " - id (same field name as the incoming record) : retrieved document id.\n" +
        " - a list of String fields containing :\n" +
        "\n" +
        "  - field name : the retrieved field name\n" +
        "  - field value : the retrieved field value"
)
@ExtraDetailFile("./details/common-processors/MultiGet-Detail.rst")
public class MultiGet extends AbstractDatastoreProcessor
{

    public static final PropertyDescriptor COLLECTION_FIELD = new PropertyDescriptor.Builder()
            .name("collection.field")
            .description("the name of the incoming records field containing es collection name to use in multiget query. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("type.field")
            .description("the name of the incoming records field containing es type name to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IDS_FIELD = new PropertyDescriptor.Builder()
            .name("ids.field")
            .description("the name of the incoming records field containing es document Ids to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("includes.field")
            .description("the name of the incoming records field containing es includes to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("excludes.field")
            .description("the name of the incoming records field containing es excludes to use in multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATASTORE_CLIENT_SERVICE);
        props.add(COLLECTION_FIELD);
        props.add(TYPE_FIELD);
        props.add(IDS_FIELD);
        props.add(INCLUDES_FIELD);
        props.add(EXCLUDES_FIELD);

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

            String collectionFieldName = context.getPropertyValue(COLLECTION_FIELD).asString();
            String typeFieldName = context.getPropertyValue(TYPE_FIELD).asString();
            String idsFieldName = context.getPropertyValue(IDS_FIELD).asString();
            String includesFieldName = context.getPropertyValue(INCLUDES_FIELD).asString();
            String excludesFieldName = context.getPropertyValue(EXCLUDES_FIELD).asString();

            List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();

            for (Record record : records) {

                if(     !record.hasField(collectionFieldName)  // record doesn't contain collection field
                        || record.getField(collectionFieldName) == null // collection field is null
                        || !record.getField(collectionFieldName).getType().equals(FieldType.STRING) // collection field is not of STRING type
                        || record.getField(collectionFieldName).getRawValue() == null // collection field raw value is null
                        || record.getField(collectionFieldName).getRawValue().toString().isEmpty() // collection field is empty
                        ) {
                    StandardRecord outputRecord = new StandardRecord(record);
                    outputRecord.addError(ProcessError.BAD_RECORD.getName(), "record must have the field "
                            + collectionFieldName + " containing the collection name to use in the multiget query. This field must be of STRING type and cannot be empty.");
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

                // collection :
                String collection = record.getField(collectionFieldName).asString();

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
                    multiGetQueryRecords.add(new MultiGetQueryRecord(collection, type, includesArray, excludesArray, idsList));
                } catch (InvalidMultiGetQueryRecordException e) {
                    StandardRecord outputRecord = new StandardRecord(record);
                    outputRecord.addError(ProcessError.BAD_RECORD.getName(), e.getMessage());
                    outputRecords.add(outputRecord);
                    continue;
                }
            }

            List<MultiGetResponseRecord> multiGetResponseRecords = datastoreClientService.multiGet(multiGetQueryRecords);

            multiGetResponseRecords.forEach(responseRecord -> {
                StandardRecord outputRecord = new StandardRecord();
                outputRecord.setStringField(collectionFieldName, responseRecord.getCollectionName());
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
