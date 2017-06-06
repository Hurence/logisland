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
package com.hurence.logisland.service.elasticsearch.multiGet;


import java.util.List;

public class MultiGetQueryRecord {

    private final String indexName;
    private final String typeName;
    private final List<String> documentIds;
    private final String[] fieldsToInclude;
    private final String[] fieldsToExclude;

    public MultiGetQueryRecord(final String indexName, final String typeName, final List<String> documentIds, final String[] fieldsToInclude, final String[] fieldsToExclude) throws InvalidMultiGetQueryRecordException {
        if(indexName == null)
            throw new InvalidMultiGetQueryRecordException("The index name cannot be null");
        if(indexName != null && indexName.isEmpty())
            throw new InvalidMultiGetQueryRecordException("The index name cannot be empty");
        if(documentIds == null)
            throw new InvalidMultiGetQueryRecordException("The list of document ids cannot be null");
        this.indexName = indexName;
        this.typeName = typeName;
        this.documentIds = documentIds;
        this.fieldsToInclude = fieldsToInclude;
        this.fieldsToExclude = fieldsToExclude;
    }

    public MultiGetQueryRecord(final String indexName, final String typeName, final List<String> documentIds)  throws InvalidMultiGetQueryRecordException {
        this(indexName, typeName, documentIds, null, null);
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public List<String> getDocumentIds() {
        return documentIds;
    }

    public String[] getFieldsToInclude() {
        return fieldsToInclude;
    }

    public String[] getFieldsToExclude() {
        return fieldsToExclude;
    }
}
