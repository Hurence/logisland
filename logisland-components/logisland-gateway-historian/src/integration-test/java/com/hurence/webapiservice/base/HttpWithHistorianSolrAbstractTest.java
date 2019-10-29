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

package com.hurence.webapiservice.base;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.http.HttpServerVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;

import static com.hurence.webapiservice.base.HistorianSolrITHelper.HISTORIAN_ADRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public abstract class HttpWithHistorianSolrAbstractTest {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpWithHistorianSolrAbstractTest.class);
    protected static WebClient webClient;
    private static int PORT = 8080;


    public static void initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        LOGGER.info("Initializing Web client");
        initializeWebClient(vertx);
        LOGGER.info("Initializing Historian solr");
        HistorianSolrITHelper.initHistorianSolr(client, container, vertx, context);
        LOGGER.info("Initializing Verticles");
        deployHttpAndHistorianVerticle(container, vertx, context).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    public static Single<String> deployHttpAndHistorianVerticle(DockerComposeContainer container, Vertx vertx, VertxTestContext context) {
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS)
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, "localhost");
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(httpConf);

        return HistorianSolrITHelper.deployHistorienVerticle(container, vertx, context)
                .flatMap(id -> vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions))
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    private static void initializeWebClient(Vertx vertx) {
        webClient = WebClient.create(vertx.getDelegate(), new WebClientOptions()
                .setDefaultHost("localhost")
                .setDefaultPort(PORT));
    }
}
