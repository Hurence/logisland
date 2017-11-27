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
package com.hurence.logisland.service.solr;

import com.hurence.logisland.annotation.documentation.CapabilityDescription;
import com.hurence.logisland.annotation.documentation.Tags;
import com.hurence.logisland.annotation.lifecycle.OnDisabled;
import com.hurence.logisland.annotation.lifecycle.OnEnabled;
import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.component.PropertyDescriptor;
import com.hurence.logisland.controller.AbstractControllerService;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.processor.ProcessException;
import com.hurence.logisland.record.Field;
import com.hurence.logisland.record.FieldType;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.record.StandardRecord;
import com.hurence.logisland.service.datastore.DatastoreClientService;
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.datastore.MultiGetQueryRecord;
import com.hurence.logisland.service.datastore.MultiGetResponseRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Tags({ "solr", "client"})
@CapabilityDescription("Implementation of ElasticsearchClientService for Solr 5.5.5.")
public class Solr_6_6_2_ClientService extends SolrClientService implements DatastoreClientService {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(Solr_6_6_2_ClientService.class);
    protected void _put(String collectionName, Record record) throws IOException, SolrServerException {
        Map<String,SolrInputField> fields = new HashMap<>();

        SolrInputField solrField = new SolrInputField(getUniqueKey(collectionName));
        solrField.setValue((Object) record.getId(), 0);
        fields.put(getUniqueKey(collectionName), solrField);
        for (Field field : record.getAllFields()) {
            if (field.isReserved()) {
                continue;
            }

            solrField = new SolrInputField(field.getName());
            solrField.setValue((Object) field.getRawValue(), 1.0f);
            fields.put(field.getName(), solrField);
        }

        SolrInputDocument document = new SolrInputDocument(fields);

        getClient().add(collectionName, document);
    }
}
