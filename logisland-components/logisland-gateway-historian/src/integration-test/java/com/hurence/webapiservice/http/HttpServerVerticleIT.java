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

package com.hurence.webapiservice.http;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.timeseries.TimeseriesVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hurence.logisland.record.FieldDictionary.*;
import static com.hurence.logisland.record.FieldDictionary.RECORD_NAME;
import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_PORT;
import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_SERVICE_NAME;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HttpServerVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticleIT.class);
    private static WebClient webClient;
    private static int PORT = 8080;

    private static String COLLECTION = "historian";


    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        LOGGER.debug("start beforeAll");
        LOGGER.info("Initializing Web client");
        webClient = WebClient.create(vertx.getDelegate(), new WebClientOptions()
                .setDefaultHost("localhost")
                .setDefaultPort(PORT));
        LOGGER.debug("Initializing ES index");
        //START COPY PASTE
        LOGGER.debug("creating collection {}", COLLECTION);
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION, "historian", 2, 1);
        client.request(createrequest);
        LOGGER.debug("verify collection {} exist and is ready", COLLECTION);
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION));
        }
        LOGGER.debug("printing conf {}", COLLECTION);
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
//        LOGGER.debug("schema is {}", new JsonArray(schema).encodePrettily());
        LOGGER.debug("Indexing some documents in {} collection", COLLECTION);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "id1");
        doc.addField(CHUNK_START, 1571129378946L);
        doc.addField(CHUNK_SIZE, 50);
        doc.addField(CHUNK_END, 1571129390801L);
        doc.addField(CHUNK_SAX, "edeebcccdf");
        doc.addField(CHUNK_VALUE, "H4sIAAAAAAAAAOPi1HzSd1wgLSvQgYtH4Ryj5nujP96La/wduLgFGDXXMgU8iLrmAZQSOMmoqVe44pJBnTtEitklrrKrxwskNYVZ01G7Nk/uhStIiknTf+ONlze1PUEcBs0Ne+2bqtabQzRpCGwtfprgCdI0kVkzR9jg8k+/YIgmwZNNH2v/OIGkTjNqpp7mPrA7zwrEO8WouSbZl3VCV5ADF6dm3rGQCfNWBkDMjm7gzl/72gNido9oxz0DRj+oAV9YPhzPv+EP0nL9ZsIdL35joCoFRk2OiNu/r7IFgFTdYdFMndd8cAKnD4g3lVnTMiPi9nUna4jZFsq7bK0VHEFSy9k0H2yRXtY4XQzqHnvThAmS/p4Qe4tdM4uypwZA7d3j9U/k+nUraIhNe/Zxr7KkBUSh7o9DvJdegRXeY9HcdOzNFE0bB4jnLYofT3fi8oIaPzvYtkrLzRSiyybm74+vDx1AUpOZNV3O1pyOWO8DcSLLgj4bMRFnkNRMZs0PvBM1d0Y7g6TYNKc/C562LMQeJHWLRdM1RqvkgDbYUZ/ZNXW9dKTqeNxBvFZWzZTCkNhbp8C8t0ya/9K0g3LinaH6JvyMX+18wQ0aPAeztf+c5vKGuuTH7pI996+6QhzpHehwxFvFFyR1hlFz0hSnxTuqbSFSQgyXmAu9AqABYnef0/KibRCIdxYYZSLCjzOqQiG+SRXjKXv/COyQ2eyaj6ef/JSdYOcgwAAAPXZ5iqICAAA=");
        doc.addField(CHUNK_AVG, 46.921143756280124d);
        doc.addField(CHUNK_MIN, 5.647955508652757d);
        doc.addField(CHUNK_WINDOW_MS, 11855);
        doc.addField(RECORD_NAME, "temp_b");
        doc.addField("tagname", "temp_b");//TODO look where it come froms
        doc.addField(CHUNK_TREND, false);
        doc.addField(CHUNK_MAX, 85.91265179503733d);
        doc.addField(CHUNK_SIZE_BYTES, 563);
        doc.addField(CHUNK_SUM, 45888);
        client.add(COLLECTION, doc);
        doc.getField("id").setValue("id2");
        doc.getField(CHUNK_START).setValue(1571129390801L);
        doc.getField(CHUNK_END).setValue(1571129490801L);
        client.add(COLLECTION, doc);
        doc.getField("id").setValue("id3");
        doc.getField(CHUNK_START).setValue(1571129490801L);
        doc.getField(CHUNK_END).setValue(1571130490801L);
        client.add(COLLECTION, doc);
        doc.getField("id").setValue("id4");
        doc.getField(RECORD_NAME).setValue("temp_a");
        client.add(COLLECTION, doc);
        UpdateResponse updateRsp = client.commit(COLLECTION);
        LOGGER.debug("Indexed some documents in {} collection", COLLECTION);
        //END COPY PASTE
        LOGGER.info("Initializing Verticles");
        String zkUrl = container.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                container.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
        JsonObject solrConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_SOLR_COLLECTION, COLLECTION)
                .put(HistorianVerticle.CONFIG_SOLR_USE_ZOOKEEPER, true)
                .put(HistorianVerticle.CONFIG_SOLR_ZOOKEEPER_URLS, new JsonArray().add(zkUrl));
        JsonObject historianConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_ROOT_SOLR, solrConf)
                .put(HistorianVerticle.CONFIG_HISTORIAN_ADDRESS, "historian_service");
        DeploymentOptions historianOptions = new DeploymentOptions().setConfig(historianConf);
        JsonObject timeseriesConf = new JsonObject()
                .put(TimeseriesVerticle.CONFIG_TIMESERIES_ADDRESS, "timeseries_service");
        DeploymentOptions timeseriesOptions = new DeploymentOptions().setConfig(timeseriesConf);
        JsonObject httpConf = new JsonObject()
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_PORT, PORT)
                .put(HttpServerVerticle.CONFIG_HISTORIAN_ADDRESS, "historian_service")
                .put(HttpServerVerticle.CONFIG_HTTP_SERVER_HOSTNAME, "localhost")
                .put(HttpServerVerticle.CONFIG_TIMESERIES_ADDRESS, "timeseries_service");
        DeploymentOptions httpOptions = new DeploymentOptions().setConfig(httpConf);

        vertx
                .rxDeployVerticle(new HistorianVerticle(), historianOptions)
                .flatMap(id -> {
                    LOGGER.info("HistorianVerticle with id '{}' deployed", id);
                    return vertx.rxDeployVerticle(new TimeseriesVerticle(), timeseriesOptions);
                })
                .flatMap(id -> {
                    LOGGER.info("TimeseriesVerticle with id '{}' deployed", id);
                    return vertx.rxDeployVerticle(new HttpServerVerticle(), httpOptions);
                }).subscribe(id -> {
                    LOGGER.info("HttpServerVerticle with id '{}' deployed", id);
                    context.completeNow();
                },
                t -> context.failNow(t));
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testErrorNoParam(VertxTestContext testContext) {
        webClient.get("/timeseries")
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(500, rsp.statusCode());
                        assertEquals("Internal Server Error", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        assertNull(body);
                        testContext.completeNow();
                    });
                }));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testGetAll(Vertx vertx, VertxTestContext testContext) {
        webClient.get("/timeseries?from=0")
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        FileSystem fs = vertx.fileSystem();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/timeseries/testGetAllResponse.json").getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
}
