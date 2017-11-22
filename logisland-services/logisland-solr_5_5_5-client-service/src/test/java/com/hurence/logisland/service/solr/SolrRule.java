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
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

/**
 * A JUnit rule which starts an embedded elastic-search instance.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class SolrRule implements TestRule {
    /**
     * An elastic-search cluster consisting of one node.
     */
    private EmbeddedSolrServer solrServer;

    /**
     * Return a closure which starts an embedded ES instance, executes the unit-test, then shuts down the
     * ES instance.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                File solrHome = new File("/home/chok/work/hurence/solr/solr-5.5.5/server/solr");
                File configFile = new File(solrHome, "solr.xml");
                CoreContainer coreContainer = new CoreContainer(solrHome.toString());
                coreContainer.load();
                solrServer = new EmbeddedSolrServer(coreContainer, "default");

                try {
                    base.evaluate(); // execute the unit test
                } finally {
                    solrServer.close();
                }
            }
        };
    }

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public SolrClient getClient() {
        return solrServer;
    }

    /**
     * When data is added to an index, it is not visible in searches until the next "refresh" has been performed.
     * Refreshes are normally done every second, but this makes it explicit..
     */
    public void refresh(String index) {
        try {

        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh index", e);
        }
    }
}