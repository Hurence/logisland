/*
 *  Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *  Copyright (c) 2017 INSA Lyon, CITI Laboratory.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hurence.webapiservice.historian;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public class HistorianVerticle extends AbstractVerticle {

  private static Logger logger = LoggerFactory.getLogger(HistorianVerticle.class);


  public static final String CONFIG_HISTORIAN_ADDRESS = "address";
  public static final String CONFIG_ROOT_SOLR = "solr";
//  public static final String CONFIG_ES_CLUSTER_NAME = "cluster.name";

  public static final String CONFIG_SOLR_URLS = "urls";
  public static final String CONFIG_SOLR_USE_ZOOKEEPER = "use_zookeeper";
  public static final String CONFIG_SOLR_ZOOKEEPER_ROOT = "zookeeper_chroot";//see zookeeper documentation about chroot
  public static final String CONFIG_SOLR_ZOOKEEPER_URLS = "zookeeper_urls";
  public static final String CONFIG_SOLR_CONNECTION_TIMEOUT = "connection_timeout";
  public static final String CONFIG_SOLR_SOCKET_TIMEOUT = "socket_timeout";
//  public static final String CONFIG_SOLR_HOST_NAME = "host";
//  public static final String CONFIG_SOLR_PORT = "port";
  public static final String CONFIG_SOLR_COLLECTION = "collection";
  private SolrClient client;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    final String address = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
    final JsonObject slrConfig = config().getJsonObject(CONFIG_ROOT_SOLR);
    final int connectionTimeout = slrConfig.getInteger(CONFIG_SOLR_CONNECTION_TIMEOUT, 10000);
    final int socketTimeout = slrConfig.getInteger(CONFIG_SOLR_SOCKET_TIMEOUT, 60000);
    final boolean useZookeeper = slrConfig.getBoolean(CONFIG_SOLR_USE_ZOOKEEPER, false);
    final String collection = slrConfig.getString(CONFIG_SOLR_COLLECTION, "historian");

    CloudSolrClient.Builder clientBuilder;
    if (useZookeeper) {
      logger.info("Zookeeper mode");
      clientBuilder = new CloudSolrClient.Builder(
                getStringListIfExist(slrConfig, CONFIG_SOLR_ZOOKEEPER_URLS).orElse(Collections.emptyList()),
                Optional.ofNullable(slrConfig.getString(CONFIG_SOLR_ZOOKEEPER_ROOT))
      );
    } else {
      logger.info("Client without zookeeper");
      clientBuilder = new CloudSolrClient.Builder(
              getStringListIfExist(slrConfig, CONFIG_SOLR_URLS)
                      .orElseThrow(IllegalArgumentException::new)
      );
    }

    this.client = clientBuilder
            .withConnectionTimeout(connectionTimeout)
            .withSocketTimeout(socketTimeout)
            .build();


    HistorianService.create(vertx, client, collection, ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(address)
                .register(HistorianService.class, ready.result());
        logger.info("{} deployed on address : '{}'", HistorianService.class.getSimpleName(), address);
        promise.complete();
      } else {
        promise.fail(ready.cause());
      }
    });
  }

  private Optional<List<String>> getStringListIfExist(JsonObject config, String key) {
    JsonArray array = config.getJsonArray(key);
    if (array == null) return Optional.empty();
    return Optional.of(array.stream()
            .map(Object::toString)
            .collect(Collectors.toList()));
  }

  @Override
  public void stop(Promise<Void> promise) throws Exception {
    if (client != null) {
      client.close();
    }
    promise.complete();
  }
}
