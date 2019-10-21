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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
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
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION, "historian",2,1);
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
        logger.debug("schema is {}", new JsonArray(schema).encodePrettily());
        logger.debug("Indexing some documents in {} collection", COLLECTION);
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "id1");
        doc.addField("chunk_start", 157062L);
        doc.addField("chunk_size", 50);
        doc.addField("chunk_end", 157062L);
        doc.addField("chunk_sax", "eebcddedcd");
        client.add(COLLECTION, doc);
        doc.getField("id").setValue("id2");
        client.add(COLLECTION, doc);
        doc.getField("id").setValue("id3");
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
    public void prepare(Vertx vertx, VertxTestContext context) throws InterruptedException {
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
        assertEquals(18, schemaRepresentation.getFields().size());
        assertEquals(69, schemaRepresentation.getDynamicFields().size());
        assertEquals(68, schemaRepresentation.getFieldTypes().size());
        assertEquals(0, schemaRepresentation.getCopyFields().size());
    }

//    @Test
//    public void testAddFieldAccuracy(SolrClient client) throws Exception {
////        CloudSolrClient.Builder clientBuilder = new CloudSolrClient.Builder(
////                Arrays.asList(slrUrl));
////
////        SolrClient client = clientBuilder
////                .withConnectionTimeout(10000)
////                .withSocketTimeout(60000)
////                .build();
//        SchemaRequest.Fields fieldsSchemaRequest = new SchemaRequest.Fields();
//        SchemaResponse.FieldsResponse initialFieldsResponse = fieldsSchemaRequest.process(client, COLLECTION);
//        assertValidSchemaResponse(initialFieldsResponse);
//
//        List<Map<String, Object>> initialFields = initialFieldsResponse.getFields();
//
//        String fieldName = "accuracyField";
//        Map<String, Object> fieldAttributes = new LinkedHashMap<>();
//        fieldAttributes.put("name", fieldName);
//        fieldAttributes.put("type", "string");
//        fieldAttributes.put("stored", false);
////        fieldAttributes.put("indexed", true);
////        fieldAttributes.put("default", "accuracy");
////        fieldAttributes.put("required", true);
//
//        SchemaRequest.AddField addFieldUpdateSchemaRequest =
//                new SchemaRequest.AddField(fieldAttributes);
//        try {
//            SchemaResponse.UpdateResponse addFieldResponse = addFieldUpdateSchemaRequest.process(client, COLLECTION);
//            assertValidSchemaResponse(addFieldResponse);
//        } catch (Exception e) {
//            logger.error("error", e);
//            logger.error("error", e);
//        }
//
//
//
//        SchemaResponse.FieldsResponse currentFieldsResponse = fieldsSchemaRequest.process(client, COLLECTION);
//        assertEquals(0, currentFieldsResponse.getStatus());
//        List<Map<String, Object>> currentFields = currentFieldsResponse.getFields();
//        assertEquals(initialFields.size() + 1, currentFields.size());
//
//
//        SchemaRequest.Field fieldSchemaRequest = new SchemaRequest.Field(fieldName);
//        SchemaResponse.FieldResponse newFieldResponse = fieldSchemaRequest.process(client, COLLECTION);
//        assertValidSchemaResponse(newFieldResponse);
//        Map<String, Object> newFieldAttributes = newFieldResponse.getField();
//        assertThat(fieldName, is(equalTo(newFieldAttributes.get("name"))));
//        assertThat("string", is(equalTo(newFieldAttributes.get("type"))));
//        assertThat(false, is(equalTo(newFieldAttributes.get("stored"))));
//        assertThat(true, is(equalTo(newFieldAttributes.get("indexed"))));
//        assertThat("accuracy", is(equalTo(newFieldAttributes.get("default"))));
//        assertThat(true, is(equalTo(newFieldAttributes.get("required"))));
//    }



    private static void assertValidSchemaResponse(SolrResponseBase schemaResponse) {
        assertEquals(0, schemaResponse.getStatus(), "Response contained errors: " + schemaResponse.toString());
        assertNull(schemaResponse.getResponse().get("errors"), "Response contained errors: " + schemaResponse.toString());
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void verifyIndexState(SolrClient client) throws Throwable {
        SolrQuery query = new SolrQuery("*:*");
        try {
            final QueryResponse response = client.query(COLLECTION, query);
            final SolrDocumentList documents = response.getResults();

            logger.info("Found " + documents.getNumFound() + " documents");
            for(SolrDocument document : documents) {
                logger.info("doc : " + document.jsonStr());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }

    }
//    @Test
//    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
//    void getSimilarDocumentsViaIdTest(VertxTestContext testContext) throws Throwable {
//        JsonObject params = new JsonObject()
//                .put("id", "2");
//        historian.rxGetTimeSeries(params)
//                .doOnError(testContext::failNow)
//                .doOnSuccess(rsp -> {
//                    testContext.verify(() -> {
//                        long totalHit = rsp.getLong("total_hit");
//                        assertEquals(107, totalHit);
//                        JsonArray docs = rsp.getJsonArray("docs");
//                        assertEquals(10, docs.size());
//                        JsonObject doc1 = docs.getJsonObject(0);
//                        assertEquals(new JsonObject()
//                                .put("author", "author1")
//                                .put("title", "title1")
//                                .put("description", "descriptionShared with guitar"), doc1);
//                        JsonObject doc2 = docs.getJsonObject(1);
//                        assertEquals(new JsonObject()
//                                .put("author", "author3")
//                                .put("title", "title3")
//                                .put("description", "descriptionShared"), doc2);
//                        testContext.completeNow();
//                    });
//                })
//                .subscribe();
//    }
//
//    @Test
//    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
//    void getDocumentsMatchingQueryTest(VertxTestContext testContext) throws Throwable {
//        JsonObject params = new JsonObject()
//                .put("query", "guitar");
//        docService.getDocumentsMatchingQuery(params, testContext.succeeding(docs -> {
//            testContext.verify(() -> {
//                assertEquals(2, docs.size());
//                JsonObject doc1 = docs.getJsonObject(0);
//                assertEquals(new JsonObject()
//                        .put("author", "author1")
//                        .put("title", "title1")
//                        .put("description", "descriptionShared with guitar"), doc1);
//                JsonObject doc2 = docs.getJsonObject(1);
//                assertEquals(new JsonObject()
//                        .put("author", "author2")
//                        .put("title", "title2")
//                        .put("description", "descriptionShared without guitar"), doc2);
//                testContext.completeNow();
//            });
//        }));
//    }


}

