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

import com.hurence.webapiservice.historian.impl.SolrHistorianConf;
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

  private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticle.class);

  //root conf
  public static final String CONFIG_HISTORIAN_ADDRESS = "address";
  public static final String CONFIG_LIMIT_NUMBER_OF_POINT = "limit_number_of_point_before_using_pre_agg";
  public static final String CONFIG_LIMIT_NUMBER_OF_CHUNK = "limit_number_of_chunks_before_using_solr_partition";
  public static final String CONFIG_ROOT_SOLR = "solr";

  //solr conf
  public static final String CONFIG_SOLR_URLS = "urls";
  public static final String CONFIG_SOLR_USE_ZOOKEEPER = "use_zookeeper";
  public static final String CONFIG_SOLR_ZOOKEEPER_ROOT = "zookeeper_chroot";//see zookeeper documentation about chroot
  public static final String CONFIG_SOLR_ZOOKEEPER_URLS = "zookeeper_urls";
  public static final String CONFIG_SOLR_CONNECTION_TIMEOUT = "connection_timeout";
  public static final String CONFIG_SOLR_SOCKET_TIMEOUT = "socket_timeout";
  public static final String CONFIG_SOLR_COLLECTION = "collection";
  public static final String CONFIG_SOLR_STREAM_ENDPOINT = "stream_url";
  public static final String CONFIG_SOLR_SLEEP_BETWEEEN_TRY = "sleep_milli_between_connection_attempt";
  public static final String CONFIG_SOLR_NUMBER_CONNECTION_ATTEMPT = "number_of_connection_attempt";

  private SolrClient client;

  @Override
  public void start(Promise<Void> promise) throws Exception {
    LOGGER.debug("deploying {} verticle with config : {}", HistorianVerticle.class.getSimpleName(), config().encodePrettily());
    //general conf
    final String address = config().getString(CONFIG_HISTORIAN_ADDRESS, "historian");
    final long limitNumberOfPoint = config().getLong(CONFIG_LIMIT_NUMBER_OF_POINT, 50000L);
    final long limitNumberOfChunks = config().getLong(CONFIG_LIMIT_NUMBER_OF_CHUNK, 50000L);
    //solr conf
    final JsonObject slrConfig = config().getJsonObject(CONFIG_ROOT_SOLR);
    final int connectionTimeout = slrConfig.getInteger(CONFIG_SOLR_CONNECTION_TIMEOUT, 10000);
    final int socketTimeout = slrConfig.getInteger(CONFIG_SOLR_SOCKET_TIMEOUT, 60000);
    final boolean useZookeeper = slrConfig.getBoolean(CONFIG_SOLR_USE_ZOOKEEPER, false);
    final String collection = slrConfig.getString(CONFIG_SOLR_COLLECTION, "historian");


    if (!slrConfig.containsKey(CONFIG_SOLR_STREAM_ENDPOINT))
      throw new IllegalArgumentException(String.format("key %s is needed in solr config of historian verticle conf.",
              CONFIG_SOLR_STREAM_ENDPOINT));
    final String streamEndpoint = slrConfig.getString(CONFIG_SOLR_STREAM_ENDPOINT);


    CloudSolrClient.Builder clientBuilder;
    if (useZookeeper) {
      LOGGER.info("Zookeeper mode");
      clientBuilder = new CloudSolrClient.Builder(
                getStringListIfExist(slrConfig, CONFIG_SOLR_ZOOKEEPER_URLS).orElse(Collections.emptyList()),
                Optional.ofNullable(slrConfig.getString(CONFIG_SOLR_ZOOKEEPER_ROOT))
      );
    } else {
      LOGGER.info("Client without zookeeper");
      clientBuilder = new CloudSolrClient.Builder(
              getStringListIfExist(slrConfig, CONFIG_SOLR_URLS)
                      .orElseThrow(IllegalArgumentException::new)
      );
    }

    this.client = clientBuilder
            .withConnectionTimeout(connectionTimeout)
            .withSocketTimeout(socketTimeout)
            .build();

    SolrHistorianConf historianConf = new SolrHistorianConf();
    historianConf.client = client;
    historianConf.collection = collection;
    historianConf.streamEndPoint = streamEndpoint;
    historianConf.limitNumberOfPoint = limitNumberOfPoint;
    historianConf.limitNumberOfChunks = limitNumberOfChunks;
    historianConf.sleepDurationBetweenTry = slrConfig.getLong(CONFIG_SOLR_SLEEP_BETWEEEN_TRY, 10000L);;
    historianConf.numberOfRetryToConnect = slrConfig.getInteger(CONFIG_SOLR_NUMBER_CONNECTION_ATTEMPT, 3);;

    HistorianService.create(vertx, historianConf, ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(address)
                .register(HistorianService.class, ready.result());
        LOGGER.info("{} deployed on address : '{}'", HistorianService.class.getSimpleName(), address);
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
