/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.elasticsearch;

// Author: Simon Kitching
// This code is in the public domain


import org.elasticsearch.client.Client;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A JUnit rule which starts an embedded elastic-search instance.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class ESRule implements TestRule {
    /**
     * An elastic-search cluster consisting of one node.
     */
    private EmbeddedElasticsearchServer eserver;

    /**
     * The internal-transport client that talks to the local node.
     */
    private Client client;

    /**
     * Return a closure which starts an embedded ES instance, executes the unit-test, then shuts down the
     * ES instance.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                eserver = new EmbeddedElasticsearchServer();
                eserver.start();

                client = eserver.getClient();
              //  loader = new ESIndicesLoader(client, 1, 1);
                try {
                    base.evaluate(); // execute the unit test
                } finally {
                    eserver.shutdown();
                }
            }
        };
    }

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public Client getClient() {
        return client;
    }

    /**
     * When data is added to an index, it is not visible in searches until the next "refresh" has been performed.
     * Refreshes are normally done every second, but this makes it explicit..
     */
    public void refresh(String index) {
        try {
            client.admin().indices().prepareRefresh(index).execute().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to refresh index", e);
        }
    }
}