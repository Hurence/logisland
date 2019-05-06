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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static junit.framework.TestCase.assertTrue;

public class SolrTokenizationIT {

    @Rule
    public SolrRule rule = new SolrRule();

    @Test
    public void testTokenizerInSolr() throws SolrServerException, IOException {
        SolrClient server = rule.getClient();
        ModifiableSolrParams params = new ModifiableSolrParams();

        // ** Let's index a document into our embedded server

        SolrInputDocument newDoc = new SolrInputDocument();
        newDoc.addField("host", "Test Document 1");
        newDoc.addField("name", "doc-1");
        newDoc.addField("type", "Hello world!");
        newDoc.addField("start", new Date().getTime());
        newDoc.addField("end", new Date().getTime() +1000);
        server.add(newDoc);
        server.commit();

        // ** And now let's query for it

        params.set("q", "name:doc-1");
        QueryResponse qResp = server.query(params);

        SolrDocumentList docList = qResp.getResults();
        assertTrue( docList.getNumFound() == 1);
        SolrDocument doc = docList.get(0);
        assertTrue( doc.getFirstValue("host").equals("Test Document 1")) ;
    }
}