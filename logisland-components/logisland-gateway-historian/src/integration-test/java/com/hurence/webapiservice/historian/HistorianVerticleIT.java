package com.hurence.webapiservice.historian;

import com.hurence.unit5.extensions.SolrExtension;
import io.vertx.core.DeploymentOptions;
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
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hurence.logisland.record.FieldDictionary.*;
import static com.hurence.unit5.extensions.SolrExtension.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianVerticleIT {

    private static Logger logger = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private com.hurence.webapiservice.historian.reactivex.HistorianService historian;
    private String zkUrl;

    public HistorianVerticleIT(DockerComposeContainer container) {
        zkUrl = container.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                container.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
    }

    @BeforeAll
    static void beforeAll(SolrClient client) throws IOException, SolrServerException {
        logger.debug("creating collection {}", COLLECTION);
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION, "historian", 2, 1);
        client.request(createrequest);
        logger.debug("verify collection {} exist and is ready", COLLECTION);
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION));
        }
        logger.debug("printing conf {}", COLLECTION);
        SchemaRequest schemaRequest = new SchemaRequest();
        SchemaResponse schemaResponse = schemaRequest.process(client, COLLECTION);
        List<Map<String, Object>> schema = schemaResponse.getSchemaRepresentation().getFields();
        logger.trace("schema is {}", new JsonArray(schema).encodePrettily());
        logger.debug("Indexing some documents in {} collection", COLLECTION);
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
        doc.getField(CHUNK_START).setValue(1571130490801L);
        doc.getField(CHUNK_END).setValue(1571130590801L);
        doc.getField(RECORD_NAME).setValue("temp_a");
        client.add(COLLECTION, doc);
        UpdateResponse updateRsp = client.commit(COLLECTION);
        logger.debug("Indexed some documents in {} collection", COLLECTION);
    }

    @AfterAll
    static void finish(SolrClient client, Vertx vertx, VertxTestContext context) throws IOException, SolrServerException {
        logger.debug("deleting collection {}", COLLECTION);
        final SolrRequest deleteRequest = CollectionAdminRequest.deleteCollection(COLLECTION);
        client.request(deleteRequest);
        logger.debug("closing vertx");
        vertx.close(context.completing());
    }

    @BeforeEach
    public void prepare(Vertx vertx, VertxTestContext context) {
        JsonObject solrConf = new JsonObject()
                .put(HistorianVerticle.CONFIG_SOLR_COLLECTION, COLLECTION)
                .put(HistorianVerticle.CONFIG_SOLR_USE_ZOOKEEPER, true)
                .put(HistorianVerticle.CONFIG_SOLR_ZOOKEEPER_URLS, new JsonArray().add(zkUrl));
        JsonObject conf = new JsonObject()
                .put(HistorianVerticle.CONFIG_ROOT_SOLR, solrConf);
        vertx.deployVerticle(new HistorianVerticle(), new DeploymentOptions().setConfig(conf),
                context.succeeding(id -> {
                    historian = com.hurence.webapiservice.historian.HistorianService.createProxy(vertx, "historian");
                    context.completeNow();
                }));
    }

    @AfterEach
    public void finish(Vertx vertx) {
        assertThat(vertx.deploymentIDs())
                .isNotEmpty()
                .hasSize(1);
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
                        assertEquals(16, doc1.size());
                        assertEquals("id1", doc1.getString("id"));
                        assertEquals(1571129378946L, doc1.getLong(CHUNK_START));
                        assertEquals(1571129390801L, doc1.getLong(CHUNK_END));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals(16, doc2.size());
                        assertEquals("id2", doc2.getString("id"));
                        assertEquals(1571129390801L, doc2.getLong(CHUNK_START));
                        assertEquals(1571129490801L, doc2.getLong(CHUNK_END));
                        JsonObject doc3 = docs.getJsonObject(2);
                        assertEquals(16, doc3.size());
                        assertEquals("id3", doc3.getString("id"));
                        assertEquals(1571129490801L, doc3.getLong(CHUNK_START));
                        assertEquals(1571130490801L, doc3.getLong(CHUNK_END));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithStart(VertxTestContext testContext) {

        JsonObject params = new JsonObject()
                .put(HistorianService.FROM, 1571129390801L);
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
                        assertEquals("id1", doc1.getString("id"));
                        JsonObject doc2 = docs.getJsonObject(1);
                        assertEquals("id2", doc2.getString("id"));
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
                        assertEquals("id1", doc1.getString("id"));
                        assertEquals(1571129378946L, doc1.getLong(CHUNK_START));
                        assertEquals(85.91265179503733d, doc1.getDouble(CHUNK_MAX));
                        assertEquals("H4sIAAAAAAAAAOPi1HzSd1wgLSvQgYtH4Ryj5nujP96La/wduLgFGDXXMgU8iLrmAZQSOMmoqVe44pJBnTtEitklrrKrxwskNYVZ01G7Nk/uhStIiknTf+ONlze1PUEcBs0Ne+2bqtabQzRpCGwtfprgCdI0kVkzR9jg8k+/YIgmwZNNH2v/OIGkTjNqpp7mPrA7zwrEO8WouSbZl3VCV5ADF6dm3rGQCfNWBkDMjm7gzl/72gNido9oxz0DRj+oAV9YPhzPv+EP0nL9ZsIdL35joCoFRk2OiNu/r7IFgFTdYdFMndd8cAKnD4g3lVnTMiPi9nUna4jZFsq7bK0VHEFSy9k0H2yRXtY4XQzqHnvThAmS/p4Qe4tdM4uypwZA7d3j9U/k+nUraIhNe/Zxr7KkBUSh7o9DvJdegRXeY9HcdOzNFE0bB4jnLYofT3fi8oIaPzvYtkrLzRSiyybm74+vDx1AUpOZNV3O1pyOWO8DcSLLgj4bMRFnkNRMZs0PvBM1d0Y7g6TYNKc/C562LMQeJHWLRdM1RqvkgDbYUZ/ZNXW9dKTqeNxBvFZWzZTCkNhbp8C8t0ya/9K0g3LinaH6JvyMX+18wQ0aPAeztf+c5vKGuuTH7pI996+6QhzpHehwxFvFFyR1hlFz0hSnxTuqbSFSQgyXmAu9AqABYnef0/KibRCIdxYYZSLCjzOqQiG+SRXjKXv/COyQ2eyaj6ef/JSdYOcgwAAAPXZ5iqICAAA=",
                                doc1.getString(CHUNK_VALUE));
                        testContext.completeNow();
                    });
                })
                .subscribe();
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void getTimeSeriesChunkTestWithName(VertxTestContext testContext) {
        JsonObject params = new JsonObject()
                .put(HistorianService.RECORD_NAME, "temp_b");
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

