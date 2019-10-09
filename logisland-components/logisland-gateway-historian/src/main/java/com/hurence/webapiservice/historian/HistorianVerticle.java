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
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="https://julien.ponge.org/">Julien Ponge</a>
 */
public class HistorianVerticle extends AbstractVerticle {

  private static Logger logger = LoggerFactory.getLogger(HistorianVerticle.class);

  public static final String CONFIG_HISTORIAN_ADDRESS = "address";
  public static final String CONFIG_ROOT_SOLR = "solr";
//  public static final String CONFIG_ES_CLUSTER_NAME = "cluster.name";
//  public static final String CONFIG_ES_NODE_HOST_NAME = "node.host";
//  public static final String CONFIG_ES_NODE_PORT_ONE = "node.port_one";
//  public static final String CONFIG_ES_NODE_PORT_TWO = "node.port_two";
//  public static final String CONFIG_ES_SCHEME = "scheme";
//  public static final String CONFIG_ES_INDEX = "index";

  public static String DEFAULT_HISTORIAN_ADDRESS = "historian";

  @Override
  public void start(Promise<Void> promise) throws Exception {
    String address = config().getString(CONFIG_HISTORIAN_ADDRESS, DEFAULT_HISTORIAN_ADDRESS);
    JsonObject slrConfig = config().getJsonObject(CONFIG_ROOT_SOLR);

    HistorianService.create(ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder.setAddress(address)
                .register(HistorianService.class, ready.result());
        logger.trace("{} deployed on address : '{}'", HistorianService.class.getSimpleName(), address);
        promise.complete();
      } else {
        promise.fail(ready.cause());
      }
    });
  }

  @Override
  public void stop(Promise<Void> promise) throws Exception {
    promise.complete();
  }
}
