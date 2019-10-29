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

import com.hurence.webapiservice.base.HistorianSolrITHelper;
import com.hurence.webapiservice.base.HttpWithHistorianSolrAbstractTest;
import com.hurence.webapiservice.base.SolrInjector;
import com.hurence.webapiservice.base.SolrInjector1;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class HttpServerVerticleSmallChunksIT extends HttpWithHistorianSolrAbstractTest {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticleSmallChunksIT.class);

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrAbstractTest
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjector injector = new SolrInjector1();
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
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
                        assertEquals(400, rsp.statusCode());
                        assertEquals("Could not parse parameter 'from' as a long. 'null' is not a long", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        assertNull(body);
                        testContext.completeNow();
                    });
                }));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testGetAllPoints(Vertx vertx, VertxTestContext testContext) {
        webClient.get("/timeseries?from=0")
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        FileSystem fs = vertx.fileSystem();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/timeseries/testSmallChunks/testGetAllPoints.json").getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testGetAllPointsFrom1To10(Vertx vertx, VertxTestContext testContext) {
        webClient.get("/timeseries?from=1&to=10")
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        FileSystem fs = vertx.fileSystem();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/timeseries/testSmallChunks/testGetAllPointsFrom1To10.json").getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testGetAllPointsFrom3To10(Vertx vertx, VertxTestContext testContext) {
        webClient.get("/timeseries?from=3&to=10")
                .as(BodyCodec.jsonObject())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonObject body = rsp.body();
                        FileSystem fs = vertx.fileSystem();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/timeseries/testSmallChunks/testGetAllPointsFrom3To10.json").getFile());
                        JsonObject expectedBody = new JsonObject(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
}
