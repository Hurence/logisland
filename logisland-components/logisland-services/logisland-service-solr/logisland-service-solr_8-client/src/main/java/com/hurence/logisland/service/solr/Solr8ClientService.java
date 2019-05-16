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
import com.hurence.logisland.service.datastore.DatastoreClientServiceException;
import com.hurence.logisland.service.solr.api.SolrClientService;
import com.hurence.logisland.service.solr.api.SolrRecordConverter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Tags({ "solr", "client"})
@CapabilityDescription("Implementation of SolrClientService for Solr 8")
public class Solr8ClientService extends SolrClientService {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(Solr8ClientService.class);

    @Override
    public SolrRecordConverter getConverter() {
        if (converter == null) {
            converter = new Solr8RecordConverter();
        }

        return super.getConverter();
    }

    protected boolean existsCloudAliasCollection(String name) throws IOException, SolrServerException {
        CollectionAdminRequest.ListAliases listAliasesRequest = new CollectionAdminRequest.ListAliases();
        CollectionAdminResponse response = listAliasesRequest.process(getClient(), name);
        if (response.getErrorMessages() != null) {
            throw new DatastoreClientServiceException("Unable to fetch collection list");
        }

        return ((ArrayList) response.getResponse().get("aliases")).contains(name);
    }

    @Override
    protected boolean existsCloudCollection(String name) throws  IOException, SolrServerException {
        return super.existsCloudCollection(name) && existsCloudAliasCollection(name);
    }

    @Override
    protected SolrClient createCloudClient(String connectionString, String collection) {
        CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(connectionString),Optional.empty()).build();
        cloudSolrClient.setDefaultCollection(collection);
        cloudSolrClient.setZkClientTimeout(30000);
        cloudSolrClient.setZkConnectTimeout(30000);

        return cloudSolrClient;
    }

    @Override
    protected SolrClient createHttpClient(String connectionString, String collection) {
        return new HttpSolrClient.Builder(connectionString + "/" + collection).build();
    }
}
