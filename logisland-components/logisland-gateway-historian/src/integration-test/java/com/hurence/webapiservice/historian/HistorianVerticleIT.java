package com.hurence.webapiservice.historian;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.base.HistorianSolrITHelper;
import com.hurence.webapiservice.base.SolrInjector;
import com.hurence.webapiservice.base.SolrInjector2;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.hurence.webapiservice.historian.HistorianService.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianVerticleIT {

    private static Logger LOGGER = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private static com.hurence.webapiservice.historian.reactivex.HistorianService historian;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, io.vertx.reactivex.core.Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HistorianSolrITHelper.initHistorianSolr(client, container, vertx, context);
        HistorianSolrITHelper
                .deployHistorienVerticle(container, vertx, context)
                .subscribe(id -> {
                    historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx.getDelegate(), "historian_service");
                    context.completeNow();
                },
                t -> context.failNow(t));
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjector injector = new SolrInjector2();
        injector.injectChunks(client);
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
        assertEquals(19, schemaRepresentation.getFields().size());
        assertEquals(69, schemaRepresentation.getDynamicFields().size());
        assertEquals(68, schemaRepresentation.getFieldTypes().size());
        assertEquals(0, schemaRepresentation.getCopyFields().size());
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithoutParameter(VertxTestContext testContext) {

        JsonObject params = new JsonObject();
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(HistorianService.TOTAL_FOUND);
                        assertEquals(4, totalHit);
                        JsonArray docs = rsp.getJsonArray(HistorianService.CHUNKS);
                        assertEquals(4, docs.size());
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertTrue(doc1.containsKey(METRIC_NAME));
                        assertTrue(doc1.containsKey(CHUNK_START));
                        assertTrue(doc1.containsKey(CHUNK_END));
                        assertTrue(doc1.containsKey(CHUNK_AVG));
                        assertTrue(doc1.containsKey(CHUNK_ID));
                        assertTrue(doc1.containsKey(CHUNK_SIZE));
                        assertTrue(doc1.containsKey(CHUNK_SAX));
                        assertTrue(doc1.containsKey(CHUNK_VALUE));
                        assertTrue(doc1.containsKey(CHUNK_MIN));
                        assertTrue(doc1.containsKey(CHUNK_MAX));
                        assertTrue(doc1.containsKey(CHUNK_WINDOW_MS));
                        assertTrue(doc1.containsKey(CHUNK_TREND));
                        assertTrue(doc1.containsKey(CHUNK_SIZE_BYTES));
                        assertTrue(doc1.containsKey(CHUNK_SUM));
                        assertTrue(doc1.containsKey(CHUNK_VERSION));
                        assertEquals(15, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(CHUNK_START));
                        assertEquals(4L, doc1.getLong(CHUNK_END));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals(15, doc2.size());
                        assertEquals("id1", doc2.getString("id"));
                        assertEquals(5L, doc2.getLong(CHUNK_START));
                        assertEquals(8L, doc2.getLong(CHUNK_END));
                        JsonObject doc3 = docs.getJsonObject(2);
                        assertEquals(15, doc3.size());
                        assertEquals("id2", doc3.getString("id"));
                        assertEquals(9L, doc3.getLong(CHUNK_START));
                        assertEquals(12L, doc3.getLong(CHUNK_END));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithStart(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(HistorianService.FROM, 9L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianService.CHUNKS);
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
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithEnd(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(HistorianService.TO, 1571129390801L);
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianService.CHUNKS);
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
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithSelectedFields(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(HistorianService.FIELDS_TO_FETCH, new JsonArray()
                    .add(CHUNK_VALUE).add(CHUNK_START).add(CHUNK_MAX).add("id")
                );
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        JsonArray docs = rsp.getJsonArray(HistorianService.CHUNKS);
                        JsonObject doc1 = docs.getJsonObject(0);
                        assertEquals(4, doc1.size());
                        assertEquals("id0", doc1.getString("id"));
                        assertEquals(1L, doc1.getLong(CHUNK_START));
                        assertEquals(8.0, doc1.getDouble(CHUNK_MAX));
                        assertEquals("H4sIAAAAAAAAAOPi1GQAAxEHLm4FRihHwYGLU9MYDD7bc3ELwMSlHAQYANb3vjkyAAAA",
                                doc1.getString(CHUNK_VALUE));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithName(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(HistorianService.NAMES, Arrays.asList("temp_a"));
        historian.rxGetTimeSeriesChunk(params)
                .doOnError(testContext::failNow)
                .doOnSuccess(rsp -> {
                    testContext.verify(() -> {
                        long totalHit = rsp.getLong(HistorianService.TOTAL_FOUND);
                        assertEquals(3, totalHit);
                        JsonArray docs = rsp.getJsonArray(HistorianService.CHUNKS);
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

