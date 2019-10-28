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

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hurence.logisland.record.FieldDictionary.*;
import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_PORT;
import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_SERVICE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HttpServerVerticleSmallChunksIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticleSmallChunksIT.class);
    private static WebClient webClient;
    private static int PORT = 8080;
    private static String COLLECTION = "historian";
    private static List<ChunkExpected> chunks = buildListOfChunks();
    private static int ddcThreshold = 0;

    HttpServerVerticleSmallChunksIT() { }

    private static List<ChunkExpected> buildListOfChunks() {
        List<ChunkExpected> chunks = new ArrayList<>();
        chunks.add(buildChunk1());
        chunks.add(buildChunk2());
        chunks.add(buildChunk3());
        return chunks;
    }

    private static ChunkExpected buildChunk1() {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = Arrays.asList(
                new Point(0, 1L, 5),
                new Point(0, 2L, 8),
                new Point(0, 3L, 1.2),
                new Point(0, 4L, 6.5)
        );
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = 1L;
        chunk.end = 4L;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.recordName = "temp_a";
        chunk.sax = "edeebcccdf";
        return chunk;
    }

    private static ChunkExpected buildChunk2() {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = Arrays.asList(
                new Point(0, 5L, -2),
                new Point(0, 6L, 8.8),
                new Point(0, 7L, 13.3),
                new Point(0, 8L, 2)
        );
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = 5L;
        chunk.end = 8L;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.recordName = "temp_a";
        chunk.sax = "edeebcccdf";
        return chunk;
    }

    private static ChunkExpected buildChunk3() {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = Arrays.asList(
                new Point(0, 9L, -5),
                new Point(0, 10L, 80),
                new Point(0, 11L, 1.2),
                new Point(0, 12L, 5.5)
        );
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = 9L;
        chunk.end = 12L;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.recordName = "temp_a";
        chunk.sax = "edeebcccdf";
        return chunk;
    }

    private static byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        LOGGER.debug("start beforeAll");
        LOGGER.info("Initializing Web client");
        initializeWebClient(vertx);
        LOGGER.debug("Initializing ES index");
        //START COPY PASTE
        LOGGER.debug("creating collection {}", COLLECTION);
        createHistorianCollection(client);
        LOGGER.debug("verify collection {} exist and is ready", COLLECTION);
        checkCollectionHasBeenCreated(client);
        LOGGER.debug("printing conf {}", COLLECTION);
        checkSchema(client);
        LOGGER.debug("Indexing some documents in {} collection", COLLECTION);
        injectChunks(client);
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

    private static void injectChunks(SolrClient client) throws SolrServerException, IOException {
        for(int i = 0; i < chunks.size(); i++) {
            ChunkExpected chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION);
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

    private static void initializeWebClient(Vertx vertx) {
        webClient = WebClient.create(vertx.getDelegate(), new WebClientOptions()
                .setDefaultHost("localhost")
                .setDefaultPort(PORT));
    }

    private static SolrInputDocument buildSolrDocument(ChunkExpected chunk, String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField(CHUNK_START, chunk.start);
        doc.addField(CHUNK_SIZE, chunk.points.size());
        doc.addField(CHUNK_END, chunk.end);
        doc.addField(CHUNK_SAX, chunk.sax);
        doc.addField(CHUNK_VALUE, chunk.compressedPoints);
        doc.addField(CHUNK_AVG, chunk.avg);
        doc.addField(CHUNK_MIN, chunk.min);
        doc.addField(CHUNK_WINDOW_MS, 11855);
        doc.addField(RECORD_NAME, chunk.recordName);
        doc.addField(CHUNK_TREND, chunk.trend);
        doc.addField(CHUNK_MAX, chunk.max);
        doc.addField(CHUNK_SIZE_BYTES, chunk.compressedPoints.length);
        doc.addField(CHUNK_SUM, chunk.sum);
        return doc;
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
