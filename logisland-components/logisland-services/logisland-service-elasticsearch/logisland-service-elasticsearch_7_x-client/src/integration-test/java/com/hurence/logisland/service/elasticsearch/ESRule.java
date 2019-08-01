 /**
 * Copyright (C) 2019 Hurence (support@hurence.com)
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

package com.hurence.logisland.service.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClient;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

 /**
 * A JUnit rule which starts an embedded elastic-search docker container.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class ESRule implements TestRule {

    /**
     * The internal-transport client that talks to the local node.
     */
    private RestHighLevelClient client;

    /**
     * Return a closure which starts an embedded ES docker container, executes the unit-test, then shuts down the
     * ES instance.
     */
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                ElasticsearchContainer container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.1.1");
                container.start();
                client = new RestHighLevelClient(RestClient.builder(HttpHost.create(container.getHttpHostAddress())));

                try {
                    base.evaluate(); // execute the unit test
                } finally {
                    client.close();
                    container.stop();
                }
            }
        };
    }

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public RestHighLevelClient getClient() {
        return client;
    }

}