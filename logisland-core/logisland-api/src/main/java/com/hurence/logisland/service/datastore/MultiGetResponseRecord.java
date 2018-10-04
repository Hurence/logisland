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
package com.hurence.logisland.service.datastore;


import java.util.Map;

public class MultiGetResponseRecord {

    private final String collectionName;
    private final String typeName;
    private final String documentId;
    private final Map<String, String> retrievedfields;


    public MultiGetResponseRecord(final String collectionName, final String typeName, final String documentId, final Map<String, String> retrievedfields) {
        this.collectionName = collectionName;
        this.typeName = typeName;
        this.documentId = documentId;
        this.retrievedfields = retrievedfields;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getDocumentId() {
        return documentId;
    }

    public Map<String, String> getRetrievedFields() {
        return retrievedfields;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(collectionName)
                .append(' ')
                .append(typeName)
                .append(' ')
                .append(retrievedfields == null?"null":retrievedfields.toString());

        return sb.toString();
    }
}
