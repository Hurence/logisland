/*
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
package com.hurence.logisland.service.solr.api;

import com.hurence.logisland.record.*;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class SolrRecordConverter {

    private static Logger logger = LoggerFactory.getLogger(SolrRecordConverter.class);

    protected SolrInputDocument createNewSolrInputDocument() {
        return new SolrInputDocument();
    }

    protected boolean isSolrDocumentFieldIgnored(String name) {
        // Begin with _ is reserved name
        return name.startsWith("_");
    }

    public Map<String, Map<String, String>> toMap(SolrDocument document, String uniqueKey) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Map<String, String> retrievedFields = new HashMap<>();
        String uniqueKeyValue = null;
        for (Map.Entry<String, Object> entry: document.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue().toString();

            if (isSolrDocumentFieldIgnored(name)) {
                continue;
            }

            if (name.equals(uniqueKey)) {
                uniqueKeyValue = value;
                continue;
            }

            retrievedFields.put(name, value);
        }

        if (uniqueKeyValue != null) {
            map.put(uniqueKeyValue, retrievedFields);
        }

        return map;
    }

    public Record toRecord(SolrDocument document, String uniqueKey) {
        Map<String, Map<String, String>> map = toMap(document, uniqueKey);
        Map.Entry<String,Map<String, String>> entry = map.entrySet().iterator().next();

        Record record = new StandardRecord();
        record.setId(entry.getKey());

        entry.getValue().forEach((key, value) -> {
            // TODO - Discover Type
            record.setField(key, FieldType.STRING, value);
        });


        return record;
    }

    public SolrInputDocument toSolrInputDocument(Record record, String uniqueKey) {
        SolrInputDocument document = createNewSolrInputDocument();

        document.addField(uniqueKey, record.getId());
        for (Field field : record.getAllFields()) {
            if (field.isReserved()) {
                continue;
            }

            document.addField(field.getName(), field.getRawValue());
        }

        return document;
    }

    public SolrInputDocument toSolrInputDocument(SolrDocument document) {
        SolrInputDocument inputDocument = createNewSolrInputDocument();

        for (String name : document.getFieldNames()) {
            inputDocument.addField(name, document.getFieldValue(name));
        }

        return inputDocument;
    }
}