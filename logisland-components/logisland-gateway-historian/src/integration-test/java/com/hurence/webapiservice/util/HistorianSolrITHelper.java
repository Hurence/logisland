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

package com.hurence.webapiservice.util;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.hurence.unit5.extensions.SolrExtension.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianSolrITHelper {

    private HistorianSolrITHelper() {}

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianSolrITHelper.class);
    public static String COLLECTION = "historian";
    public static String HISTORIAN_ADRESS = "historian_service";

    public static void initHistorianSolr(SolrClient client) throws IOException, SolrServerException {
        LOGGER.debug("creating collection {}", COLLECTION);
        createHistorianCollection(client);
        LOGGER.debug("verify collection {} exist and is ready", COLLECTION);
        checkCollectionHasBeenCreated(client);
        LOGGER.debug("printing conf {}", COLLECTION);
        checkSchema(client);
    }

    @BeforeAll
    public static void initHistorianAndDeployVerticle(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        initHistorianSolr(client);
        LOGGER.info("Initializing Verticles");
        deployHistorienVerticle(container, vertx).subscribe(id -> {
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    public static Single<String> deployHistorienVerticle(DockerComposeContainer container, Vertx vertx) {
        DeploymentOptions historianOptions = getDeploymentOptions(container);
        return vertx.rxDeployVerticle(new HistorianVerticle(), historianOptions)
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static Single<String> deployHistorienVerticle(DockerComposeContainer container,
                                                         Vertx vertx,
                                                         JsonObject customHistorianConf) {
        DeploymentOptions historianOptions = getDeploymentOptions(container, customHistorianConf);
        return vertx.rxDeployVerticle(new HistorianVerticle(), historianOptions)
                .map(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return id;
                });
    }

    public static DeploymentOptions getDeploymentOptions(DockerComposeContainer container) {
        JsonObject historianConf = getHistorianConf(container);
        return new DeploymentOptions().setConfig(historianConf);
    }

    private static JsonObject getHistorianConf(DockerComposeContainer container) {
        String zkUrl = container.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                container.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
        String slr1Url = container.getServiceHost(SOLR1_SERVICE_NAME, SOLR_1_PORT)
                + ":" +
                container.getServicePort(SOLR1_SERVICE_NAME, SOLR_1_PORT);

        JsonObject solrConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_SOLR_COLLECTION, COLLECTION)
                .put(HistorianVerticle.CONFIG_SOLR_USE_ZOOKEEPER, true)
                .put(HistorianVerticle.CONFIG_SOLR_ZOOKEEPER_URLS, new JsonArray().add(zkUrl))
                .put(HistorianVerticle.CONFIG_SOLR_STREAM_ENDPOINT, "http://" + slr1Url + "/solr/" + COLLECTION);
        return new JsonObject()
                .put(HistorianVerticle.CONFIG_ROOT_SOLR, solrConf)
                .put(HistorianVerticle.CONFIG_HISTORIAN_ADDRESS, HISTORIAN_ADRESS);
    }

    public static DeploymentOptions getDeploymentOptions(DockerComposeContainer container,
                                                         JsonObject customHistorianConf) {
        JsonObject historianConf = getHistorianConf(container);
        return new DeploymentOptions().setConfig(historianConf.mergeIn(customHistorianConf));
    }

    private static void checkSchema(SolrClient client) throws SolrServerException, IOException {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
//        LOGGER.debug("schema is {}", new JsonArray(schema).encodePrettily());
    }

    private static void checkCollectionHasBeenCreated(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION));
        }
    }

    private static void createHistorianCollection(SolrClient client) throws SolrServerException, IOException {
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION, "historian", 2, 1);
        client.request(createrequest);
    }
}
