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

@Category(ComponentCategory.ENRICHMENT)
@Tags({"datastore", "enricher"})
@CapabilityDescription("Enrich input records with content indexed in datastore using multiget queries.\n" +
        "Each incoming record must be possibly enriched with information stored in datastore. \n" +
        "The plugin properties are :\n" +
        "\n" +
        "- es.index (String)            : Name of the datastore index on which the multiget query will be performed. This field is mandatory and should not be empty, otherwise an error output record is sent for this specific incoming record.\n" +
        "- record.key (String)          : Name of the field in the input record containing the id to lookup document in elastic search. This field is mandatory.\n" +
        "- es.key (String)              : Name of the datastore key on which the multiget query will be performed. This field is mandatory.\n" +
        "- includes (ArrayList<String>) : List of patterns to filter in (include) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "- excludes (ArrayList<String>) : List of patterns to filter out (exclude) fields to retrieve. Supports wildcards. This field is not mandatory.\n" +
        "\n" +
        "Each outcoming record holds at least the input record plus potentially one or more fields coming from of one datastore document."
)
@ExtraDetailFile("./details/common-processors/EnrichRecords-Detail.rst")
public class EnrichRecords extends AbstractDatastoreProcessor {

    public static final PropertyDescriptor RECORD_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("record.key")
            .description("The name of field in the input record containing the document id to use in ES multiget query")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("collection.name")
            .description("The name of the collection to look for")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor TYPE_NAME = new PropertyDescriptor.Builder()
            .name("type.name")
            .description("The typle of record to look for")
            .required(false)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor INCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("includes.field")
            .description("The name of the ES fields to include in the record.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("*")
            .build();

    public static final PropertyDescriptor EXCLUDES_FIELD = new PropertyDescriptor.Builder()
            .name("excludes.field")
            .description("The name of the ES fields to exclude.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("N/A")
            .build();

    private static final String ATTRIBUTE_MAPPING_SEPARATOR = ":";
    private static final String ATTRIBUTE_MAPPING_SEPARATOR_REGEXP = "\\s*" + ATTRIBUTE_MAPPING_SEPARATOR + "\\s*";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATASTORE_CLIENT_SERVICE);
        props.add(RECORD_KEY_FIELD);
        props.add(INCLUDES_FIELD);
        props.add(EXCLUDES_FIELD);
        props.add(TYPE_NAME);
        props.add(COLLECTION_NAME);

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
        List<Triple<Record, String, IncludeFields>> recordsToEnrich = new ArrayList<>();

        if (records.size() != 0) {
            String excludesFieldName = context.getPropertyValue(EXCLUDES_FIELD).asString();

            // Excludes :
            String[] excludesArray = null;
            if ((excludesFieldName != null) && (!excludesFieldName.isEmpty())) {
                excludesArray = excludesFieldName.split("\\s*,\\s*");
            }

            //List<MultiGetQueryRecord> multiGetQueryRecords = new ArrayList<>();
            MultiGetQueryRecordBuilder mgqrBuilder = new MultiGetQueryRecordBuilder();

            mgqrBuilder.excludeFields(excludesArray);

            List<MultiGetResponseRecord> multiGetResponseRecords = null;
            // HashSet<String> ids = new HashSet<>(); // Use a Set to avoid duplicates

            for (Record record : records) {

                String recordKeyName = null;
                String indexName = null;
                String typeName = FieldDictionary.RECORD_TYPE;
                String includesFieldName = null;

                try {
                    recordKeyName = context.getPropertyValue(RECORD_KEY_FIELD).evaluate(record).asString();
                    indexName = context.getPropertyValue(COLLECTION_NAME).evaluate(record).asString();
                    if (context.getPropertyValue(TYPE_NAME).isSet())
                        typeName = context.getPropertyValue(TYPE_NAME).evaluate(record).asString();
                    includesFieldName = context.getPropertyValue(INCLUDES_FIELD).evaluate(record).asString();
                } catch (Throwable t) {
                    record.setStringField(FieldDictionary.RECORD_ERRORS, "Failure in executing EL. Error: " + t.getMessage());
                    getLogger().error("Cannot interpret EL : " + record, t);
                }


                if (recordKeyName != null) {
                    try {
                        String key = record.getField(recordKeyName).asString();
                        // Includes :
                        String[] includesArray = null;
                        if ((includesFieldName != null) && (!includesFieldName.isEmpty())) {
                            includesArray = includesFieldName.split("\\s*,\\s*");
                        }
                        IncludeFields includeFields = new IncludeFields(includesArray);
                        mgqrBuilder.add(indexName, typeName, includeFields.getAttrsToIncludeArray(), key);


                        recordsToEnrich.add(new ImmutableTriple(record, asUniqueKey(indexName, typeName, key), includeFields));
                    } catch (Throwable t) {
                        record.setStringField(FieldDictionary.RECORD_ERRORS, "Can not request datastore with " + indexName + " " + typeName + " " + recordKeyName);
                        outputRecords.add(record);
                    }
                } else {
                    //record.setStringField(FieldDictionary.RECORD_ERRORS, "Interpreted EL returned null for recordKeyName");
                    outputRecords.add(record);
                    //logger.error("Interpreted EL returned null for recordKeyName");
                }
            }

            try {
                List<MultiGetQueryRecord> mgqrs = mgqrBuilder.build();

                multiGetResponseRecords = datastoreClientService.multiGet(mgqrs);
            } catch (InvalidMultiGetQueryRecordException e) {
                getLogger().error("error while multiger", e);
            }

            if (multiGetResponseRecords == null || multiGetResponseRecords.isEmpty()) {
                return records;
            }


            // Transform the returned documents from ES in a Map
            Map<String, MultiGetResponseRecord> responses = multiGetResponseRecords.
                    stream().
                    collect(Collectors.toMap(EnrichRecords::asUniqueKey, Function.identity()));

            recordsToEnrich.forEach(recordToEnrich -> {

                Triple<Record, String, IncludeFields> triple = recordToEnrich;
                Record outputRecord = triple.getLeft();

                // TODO: should probably store the resulting recordKeyName during previous invocation above
                String key = triple.getMiddle();
                IncludeFields includeFields = triple.getRight();

                MultiGetResponseRecord responseRecord = responses.get(key);
                if ((responseRecord != null) && (responseRecord.getRetrievedFields() != null)) {
                    // Retrieve the fields from responseRecord that matches the ones in the recordToEnrich.
                    responseRecord.getRetrievedFields().forEach((k, v) -> {
                        String fieldName = k.toString();
                        if (includeFields.includes(fieldName)) {
                            // Now check if there is an attribute mapping rule to apply
                            if (includeFields.hasMappingFor(fieldName)) {
                                String mappedAttributeName = includeFields.getAttributeToMap(fieldName);
                                // Replace the attribute name
                                outputRecord.setStringField(mappedAttributeName, v.toString());
                            } else {
                                outputRecord.setStringField(fieldName, v.toString());
                            }
                        }
                    });
                }
                outputRecords.add(outputRecord);

            });
        }
        return outputRecords;
    }

    /*
     * Returns true if the array of attributes to include contains at least one attribute mapping
     */
    private boolean hasAttributeMapping(String[] includesArray) {
        boolean attrMapping = false;
        for (String includePattern : includesArray) {
            if (includePattern.contains(":")) {
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
        public IncludeFields(String[] includesArray) {
            if (includesArray == null || includesArray.length <= 0) {
                containsAll = true;
                this.attrsToIncludeArray = includesArray;
            } else {
                for (String includePattern : includesArray) {
                    if (includePattern.contains(ATTRIBUTE_MAPPING_SEPARATOR)) {
                        hasAttributeMapping = true;
                        attrsToIncludeList = new HashSet<String>();
                        attributesMapping = new HashMap<String, String>();
                        break;
                    }
                }

                Arrays.stream(includesArray).forEach((k) -> {
                    if (k.equals("*")) {
                        containsAll = true;
                        if (hasAttributeMapping) {
                            attrsToIncludeList.add(k);
                        }
                    } else if (k.contains("*")) {
                        // It is a substring
                        if (containsSubstring == false) {
                            substringFields = new HashMap<>();
                            containsSubstring = true;
                        }
                        String buildCompileStr = k.replaceAll("\\*", ".\\*");
                        Pattern pattern = Pattern.compile(buildCompileStr);
                        substringFields.put(k, pattern);
                        if (hasAttributeMapping) {
                            attrsToIncludeList.add(k);
                        }
                    } else {
                        if (containsEquality == false) {
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
                            } else {
                                equalityFields.add(k);
                                attrsToIncludeList.add(k);
                            }
                        } else {
                            equalityFields.add(k);
                        }
                    }
                });
                if (hasAttributeMapping) {
                    this.attrsToIncludeArray = (String[]) attrsToIncludeList.toArray(new String[attrsToIncludeList.size()]);
                } else {
                    this.attrsToIncludeArray = includesArray;
                }
            }
        }

        public boolean hasMappingFor(String attr) {
            if ((attributesMapping == null) || attributesMapping.isEmpty() || (attr == null)) {
                return false;
            }
            return attributesMapping.containsKey(attr);
        }

        public String getAttributeToMap(String attr) {
            if ((attributesMapping == null) || attributesMapping.isEmpty() || (attr == null)) {
                return null;
            }
            return attributesMapping.get(attr);
        }

        public boolean matchesAll() {
            return containsAll;
        }

        public boolean containsSubstring() {
            return containsSubstring;
        }

        public boolean containsEquality() {
            return containsEquality;
        }

        public boolean includes(String fieldName) {
            if (containsAll) {
                return true;
            }
            if (containsEquality) {
                if (equalityFields.contains(fieldName)) {
                    return true;
                }
            }
            if (containsSubstring) {
                // Must go through each substring expresssion
                for (String key : substringFields.keySet()) {
                    Pattern expr = substringFields.get(key);
                    Matcher valueMatcher = expr.matcher(fieldName);
                    if (expr != null) {
                        try {
                            if (valueMatcher.lookingAt()) {
                                return true;
                            }
                        } catch (Exception e) {
                            getLogger().warn("issue while matching on fieldname {}", new Object[]{fieldName}, e);
                        }
                    }
                }
            }
            return false;
        }
    }
}