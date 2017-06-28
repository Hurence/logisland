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
package com.hurence.logisland.service.elasticsearch;

// Author: Simon Kitching
// This code is in the public domain


import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;

/**
 * Test helper class which starts up an Elasticsearch instance in the current JVM.
 */
public class EmbeddedElasticsearchServer {

    // Suitable location for use with Maven
    private static final String DEFAULT_HOME_DIRECTORY = "target/elasticsearch-home";
    public static final String CLUSTER_NAME = "testCluster";

    // The embedded ES instance
    private final Node node;

    // Setting "path.home" should point to the directory in which Elasticsearch is installed.
    private final String homeDirectory;

    /**
     * Default Constructor.
     */
    public EmbeddedElasticsearchServer() {
        this(DEFAULT_HOME_DIRECTORY);
    }

    /**
     * Explicit Constructor.
     */
    public EmbeddedElasticsearchServer(String homeDirectory) {
        try {
            FileUtils.deleteDirectory(new File(homeDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Unable to clean embedded elastic-search home dir", e);
        }

        this.homeDirectory = homeDirectory;

        Settings.Builder elasticsearchSettings = Settings.builder()

                .put("cluster.name", CLUSTER_NAME)
                .put("node.name", "testNode")
                .put("number_of_shards", 3)
                .put("number_of_replicas", 1)
                .put("network.host", "local")
                .put("node.data", true)
                .put("node.local", true)
                .put("http.enabled", false)
                .put("path.home", homeDirectory)
                .put("discovery.zen.ping_timeout", 0); // make startup faster

        /*# Using less filesystem as possible
index.store.type=memory
index.store.fs.memory.enabled=true
index.gateway.type=none
gateway.type=none*/

        this.node = new Node(elasticsearchSettings.build());
    }

    public void start() throws Exception {
        this.node.start();
    }

    public Client getClient() {
        return node.client();
    }

    public void shutdown() throws IOException {
        node.close();

        try {
            FileUtils.deleteDirectory(new File(homeDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Could not delete home directory of embedded elasticsearch server", e);
        }
    }
}