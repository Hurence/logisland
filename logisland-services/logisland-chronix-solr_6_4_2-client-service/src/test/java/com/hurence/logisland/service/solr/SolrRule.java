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

// Author: Simon Kitching
// This code is in the public domain


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;

/**
 * A JUnit rule which starts an embedded elastic-search instance.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class SolrRule extends ExternalResource {

    private EmbeddedSolrServer server;
    private CoreContainer container;

    @Override
    protected void before() throws Throwable {
        container = new CoreContainer("src/test/resources/solr");
        container.load();

        server = new EmbeddedSolrServer(container, "chronix" );

        getClient().deleteByQuery("*:*");
        getClient().commit();
    };

    @Override
    protected void after() {
        try {
            server.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public SolrClient getClient() {
        return server;
    }


}