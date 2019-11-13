package com.hurence.webapiservice.historian;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianFields.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.initHistorianSolr(client);
        HistorianSolrITHelper
                .deployHistorienVerticle(container, vertx)
                .subscribe(id -> {
                    historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), "historian_service");
                    context.completeNow();
                },
                t -> context.failNow(t));
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags injectorTempA = new SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(
                "temp_a",
                Arrays.asList(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()
                ),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 1L, 5),
                                new Point(0, 2L, 8),
                                new Point(0, 3L, 1.2),
                                new Point(0, 4L, 6.5)
                        ),
                        Arrays.asList(
                                new Point(0, 5L, -2),
                                new Point(0, 6L, 8.8),
                                new Point(0, 7L, 13.3),
                                new Point(0, 8L, 2)
                        ),
                        Arrays.asList(
                                new Point(0, 9L, -5),
                                new Point(0, 10L, 80),
                                new Point(0, 11L, 1.2),
                                new Point(0, 12L, 5.5)
                        )
                ));
        SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags injectorTempB = new SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(
                "temp_b",
                Arrays.asList(
                        Collections.emptyList()
                ),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 9L, -5),
                                new Point(0, 10L, 80),
                                new Point(0, 11L, 1.2),
                                new Point(0, 12L, 5.5)
                        )
                ));
        injectorTempA.addChunk(injectorTempB);
        injectorTempA.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
    }

    @AfterAll
    static void finish(SolrClient client, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        LOGGER.debug("deleting collection {}", COLLECTION);
        final SolrRequest deleteRequest = CollectionAdminRequest.deleteCollection(COLLECTION);
        client.request(deleteRequest);
        LOGGER.debug("closing vertx");
        vertx.close(context.completing());
    }

    @Test
    public void testSchemaRequest(SolrClient client) throws Exception {
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION);
        assertValidSchemaResponse(schemaResponse);
        SchemaRepresentation schemaRepresentation = schemaResponse.getSchemaRepresentation();
        assertNotNull(schemaRepresentation);
        assertEquals("historian", schemaRepresentation.getName());
        assertEquals(1.6, schemaRepresentation.getVersion(), 0.001f);
        assertEquals("id", schemaRepresentation.getUniqueKey());
        assertEquals(20, schemaRepresentation.getFields().size());
        assertEquals(69, schemaRepresentation.getDynamicFields().size());
        assertEquals(68, schemaRepresentation.getFieldTypes().size());
        assertEquals(0, schemaRepresentation.getCopyFields().size());
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithoutParameter(VertxTestContext testContext) {

        JsonObject params = new JsonObject();
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(RESPONSE_TOTAL_FOUND);
                        assertEquals(4, totalHit);
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        assertEquals(4, docs.size());
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertTrue(doc1.containsKey(RESPONSE_METRIC_NAME_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_START_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_END_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_AVG_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_ID_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SAX_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VALUE_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_MIN_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_MAX_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_WINDOW_MS_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_TREND_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SIZE_BYTES_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_SUM_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_VERSION_FIELD));
                        assertTrue(doc1.containsKey(RESPONSE_CHUNK_FIRST_VALUE_FIELD));
                        assertEquals(16, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(RESPONSE_CHUNK_START_FIELD));
                        assertEquals(4L, doc1.getLong(RESPONSE_CHUNK_END_FIELD));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("id1", doc2.getString("id"));
                        assertEquals(5L, doc2.getLong(RESPONSE_CHUNK_START_FIELD));
                        assertEquals(8L, doc2.getLong(RESPONSE_CHUNK_END_FIELD));
                        JsonObject doc3 = docs.getJsonObject(2);
                        assertEquals("id2", doc3.getString("id"));
                        assertEquals(9L, doc3.getLong(RESPONSE_CHUNK_START_FIELD));
                        assertEquals(12L, doc3.getLong(RESPONSE_CHUNK_END_FIELD));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithStart(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(FROM_REQUEST_FIELD, 9L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        JsonObject doc2 = docs.getJsonObject(0);
                        assertEquals("id2", doc2.getString("id"));
                        JsonObject doc3 = docs.getJsonObject(1);
                        assertEquals("id3", doc3.getString("id"));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithEnd(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(TO_REQUEST_FIELD, 1571129390801L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals("id0", doc1.getString("id"));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("id1", doc2.getString("id"));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    @Disabled("This feature is legacy, now this is the service that decides what to return based on timeseries request.")
    void getTimeSeriesChunkTestWithSelectedFields(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(FIELDS_TO_FETCH_AS_LIST_REQUEST_FIELD, new JsonArray()
                    .add(RESPONSE_CHUNK_VALUE_FIELD).add(RESPONSE_CHUNK_START_FIELD).add(RESPONSE_CHUNK_MAX_FIELD).add("id")
                );
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals(4, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(RESPONSE_CHUNK_START_FIELD));
                        assertEquals(8.0, doc1.getDouble(RESPONSE_CHUNK_MAX_FIELD));
                        assertEquals("H4sIAAAAAAAAAOPi1GQAAxEHLm4FRihHwYGLU9MYDD7bc3ELwMSlHAQYANb3vjkyAAAA",
                                doc1.getString(RESPONSE_CHUNK_VALUE_FIELD));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithName(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(METRIC_NAMES_AS_LIST_REQUEST_FIELD, Arrays.asList("temp_a"));
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(RESPONSE_TOTAL_FOUND);
                        assertEquals(3, totalHit);
                        JsonArray docs = rsp.getJsonArray(RESPONSE_CHUNKS);
                        assertEquals(3, docs.size());
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
        assertEquals(0, schemaResponse.getStatus(), "Response contained errors: " + schemaResponse.toString());
        assertNull(schemaResponse.getResponse().get("errors"), "Response contained errors: " + schemaResponse.toString());
    }

}

