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
import com.hurence.logisland.record.FieldDictionary;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.datastore.InvalidMultiGetQueryRecordException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetQueryRecordBuilder;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import com.hurence.logisland.validator.StandardValidators;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"elasticsearch"})
@CapabilityDescription("Enrich input records with content indexed in elasticsearch using multiget queries.\n" +
        "Each incoming record must be possibly enriched with information stored in elasticsearch. \n" +
        "Each outcoming record holds at least the input record plus potentially one or more fields coming from of one elasticsearch document."
)
public class EnrichRecordsElasticsearch extends AbstractElasticsearchProcessor {

    public static final PropertyDescriptor RECORD_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("record.key")
            .description("The name of field in the input record containing the document id to use in ES multiget query")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ES_INDEX_FIELD = new PropertyDescriptor.Builder()
            .name("es.index")
            .description("The name of the ES index to use in multiget query. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();


    public static final PropertyDescriptor ES_TYPE_FIELD = new PropertyDescriptor.Builder()
            .name("es.type")
            .description("The name of the ES type to use in multiget query.")
            .required(false)
            .defaultValue("default")
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ES_INCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.includes.field")
            .description("The name of the ES fields to include in the record.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("*")
            .build();

    public static final PropertyDescriptor ES_EXCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("es.excludes.field")
            .description("The name of the ES fields to exclude.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("N/A")
            .build();

    private static final String ATTRIBUTE_MAPPING_SEPARATOR = ":";
    private static final String ATTRIBUTE_MAPPING_SEPARATOR_REGEXP = "\\s*"+ATTRIBUTE_MAPPING_SEPARATOR+"\\s*";

    private String[] excludesArray = null;

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


    @Override
    public void init(final ProcessContext context) {
        super.init(context);
        String excludesFieldName = context.getPropertyValue(ES_EXCLUDES_FIELD).asString();
        if ((excludesFieldName != null) && (!excludesFieldName.isEmpty())) {
            excludesArray = excludesFieldName.split("\\s*,\\s*");
        }
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
        if (records.size() == 0) {
            return Collections.emptyList();
        }

        List<Triple<Record, String, IncludeFields>> recordsToEnrich = new ArrayList<>();
        MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();

        mgqrBuilder.excludeFields(excludesArray);

        for (Record record : records) {

            String recordKeyName = evaluatePropAsString(record, context, RECORD_KEY_FIELD);
            String indexName = evaluatePropAsString(record, context, ES_INDEX_FIELD);
            String typeName = evaluatePropAsString(record, context, ES_TYPE_FIELD);
            String includesFieldName = evaluatePropAsString(record, context, ES_INCLUDES_FIELD);

            if (recordKeyName != null && indexName != null && typeName != null) {
                try {
                    // Includes :
                    String[] includesArray = null;
                    if ((includesFieldName != null) && (!includesFieldName.isEmpty())) {
                        includesArray = includesFieldName.split("\\s*,\\s*");
                    }
                    IncludeFields includeFields = new IncludeFields(includesArray);
                    mgqrBuilder.add(indexName, typeName, includeFields.getAttrsToIncludeArray(), recordKeyName);
                    recordsToEnrich.add(new ImmutableTriple(record, asUniqueKey(indexName, typeName, recordKeyName), includeFields));
                } catch (Throwable t) {
                    record.setStringField(FieldDictionary.RECORD_ERRORS, "Can not request ElasticSearch with " + indexName + " "  + typeName + " " + recordKeyName);
                    getLogger().error("Can not request ElasticSearch with index: {}, type: {}, recordKey: {}, record id is :\n{}",
                            new Object[]{ indexName, typeName, recordKeyName, record.getId() },
                            t);
                }
            } else {
                getLogger().warn("Can not request ElasticSearch with " +
                        "index: {}, type: {}, recordKey: {}, record id is :\n{}",
                        new Object[]{ indexName, typeName, recordKeyName, record.getId() });
            }
        }

        List<MultiGetResponseRecord> multiGetResponseRecords = null;
        try {
            List<MultiGetQueryRecord> mgqrs = mgqrBuilder.build();
            if (mgqrs.isEmpty()) return records;
            multiGetResponseRecords = elasticsearchClientService.multiGet(mgqrs);
        } catch (InvalidMultiGetQueryRecordException e ) {
            getLogger().error("error while multiGet elasticsearch", e);
        }

        if (multiGetResponseRecords == null || multiGetResponseRecords.isEmpty()) {
            return records;
        }


        // Transform the returned documents from ES in a Map
        Map<String, MultiGetResponseRecord> responses = multiGetResponseRecords.
                stream().
                collect(Collectors.toMap(EnrichRecordsElasticsearch::asUniqueKey, Function.identity()));

        recordsToEnrich.forEach(recordToEnrich -> {

            Triple<Record, String, IncludeFields> triple = recordToEnrich;
            Record outputRecord = triple.getLeft();
            String key = triple.getMiddle();
            IncludeFields includeFields = triple.getRight();

            MultiGetResponseRecord responseRecord = responses.get(key);
            if ((responseRecord != null) && (responseRecord.getRetrievedFields() != null)) {
                // Retrieve the fields from responseRecord that matches the ones in the recordToEnrich.
                responseRecord.getRetrievedFields().forEach((fieldName, v) -> {
                    if (includeFields.includes(fieldName)) {
                        // Now check if there is an attribute mapping rule to apply
                        if (includeFields.hasMappingFor(fieldName)){
                            String mappedAttributeName = includeFields.getAttributeToMap(fieldName);
                            // Replace the attribute name
                            outputRecord.setStringField(mappedAttributeName, v);
                        }
                        else {
                            outputRecord.setStringField(fieldName, v);
                        }
                    }
                });
            }
        });

        return records;
    }

    private String evaluatePropAsString(Record record, ProcessContext context, PropertyDescriptor descriptor) {
        try {
            return context.getPropertyValue(descriptor).evaluate(record).asString();
        } catch (Throwable t) {
            record.addError(ProcessError.RECORD_CONVERSION_ERROR.toString(),
                    " :Failure in executing EL for property '"+
                            descriptor.getName() + "'. Error: " + t.getMessage());
            getLogger().error("Cannot interpret EL for property '{}' and record : '{}'",
                    new Object[]{ descriptor.getName(), record }, t);
            return null;
        }
    }
    /*
     * Returns true if the array of attributes to include contains at least one attribute mapping
     */
    private boolean hasAttributeMapping(String[] includesArray){
        boolean attrMapping = false;
        for (String includePattern : includesArray){
            if (includePattern.contains(":")){
                return true;
            }
        }
        return false;
    }

    private static String asUniqueKey(MultiGetResponseRecord mgrr) {
        return asUniqueKey(mgrr.getCollectionName(), mgrr.getTypeName(), mgrr.getDocumentId());
    }

    private static String asUniqueKey(String indexName, String typeName, String documentId) {
        StringBuilder sb = new StringBuilder();
        sb.append(indexName)
                .append(":")
                .append(typeName)
                .append(":")
                .append(documentId);
        return sb.toString();
    }

    class IncludeFields {
        private HashMap<String, String> attributesMapping = null;
        boolean hasAttributeMapping = false;
        private Set<String> attrsToIncludeList = null;

        public String[] getAttrsToIncludeArray() {
            return attrsToIncludeArray;
        }

        private String[] attrsToIncludeArray = null;

        private boolean containsAll = false;
        private boolean containsSubstring = false;
        private boolean containsEquality = false;

        Set<String> equalityFields = null;
        Map<String, Pattern> substringFields = null;

        /*
         * Constructor
         */
        public IncludeFields(String[] includesArray){
            if (includesArray == null || includesArray.length <= 0){
                containsAll = true;
                this.attrsToIncludeArray = includesArray;
            }
            else {
                for (String includePattern : includesArray){
                    if (includePattern.contains(ATTRIBUTE_MAPPING_SEPARATOR)){
                        hasAttributeMapping = true;
                        attrsToIncludeList = new HashSet<String>();
                        attributesMapping = new HashMap<String, String>();
                        break;
                    }
                }

                Arrays.stream(includesArray).forEach((k) -> {
                    if (k.equals("*")){
                        containsAll = true;
                        if (hasAttributeMapping) {
                            attrsToIncludeList.add(k);
                        }
                    }
                    else if (k.contains("*")){
                        // It is a substring
                        if (containsSubstring == false){
                            substringFields = new HashMap<>();
                            containsSubstring = true;
                        }
                        String buildCompileStr = k.replaceAll("\\*", ".\\*");
                        Pattern pattern = Pattern.compile(buildCompileStr);
                        substringFields.put(k, pattern);
                        if (hasAttributeMapping) {
                            attrsToIncludeList.add(k);
                        }
                    }
                    else {
                        if (containsEquality == false){
                            equalityFields = new HashSet<String>();
                            containsEquality = true;
                        }
                        if (hasAttributeMapping) {
                            if (k.contains(ATTRIBUTE_MAPPING_SEPARATOR)) {
                                String[] splited = k.split(ATTRIBUTE_MAPPING_SEPARATOR_REGEXP);
                                if (splited.length == 2) {
                                    attrsToIncludeList.add(splited[1]);
                                    attributesMapping.put(splited[1], splited[0]);
                                    equalityFields.add(splited[1]);
                                }
                            }
                            else {
                                equalityFields.add(k);
                                attrsToIncludeList.add(k);
                            }
                        }
                        else {
                            equalityFields.add(k);
                        }
                    }
                });
                if (hasAttributeMapping){
                    this.attrsToIncludeArray = (String [])attrsToIncludeList.toArray(new String[attrsToIncludeList.size()]);
                }
                else {
                    this.attrsToIncludeArray = includesArray;
                }
            }
        }

        public boolean hasMappingFor(String attr){
            if ((attributesMapping == null) || attributesMapping.isEmpty() || (attr == null)) {
                return false;
            }
            return attributesMapping.containsKey(attr);
        }

        public String getAttributeToMap(String attr){
            if ((attributesMapping == null) || attributesMapping.isEmpty() || (attr == null)) {
                return null;
            }
            return attributesMapping.get(attr);
        }

        public boolean matchesAll() {
            return containsAll;
        }

        public boolean containsSubstring(){
            return containsSubstring;
        }

        public boolean containsEquality(){
            return containsEquality;
        }

        public boolean includes(String fieldName){
            if (containsAll){
                return true;
            }
            if (containsEquality) {
                if (equalityFields.contains(fieldName)) {
                    return true;
                }
            }
            if (containsSubstring){
                // Must go through each substring expresssion
                for (String key : substringFields.keySet()) {
                    Pattern expr = substringFields.get(key);
                    Matcher valueMatcher = expr.matcher(fieldName);
                    if (expr != null){
                        try{
                            if (valueMatcher.lookingAt()){
                                return true;
                            }
                        }
                        catch(Exception e){
                            getLogger().warn("issue while matching on fieldname {}", new Object[]{fieldName}, e);
                        }
                    }
                }
            }
            return false;
        }
    }
}