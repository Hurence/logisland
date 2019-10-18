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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_PORT;
import static com.hurence.unit5.extensions.SolrExtension.ZOOKEEPER_SERVICE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class HistorianVerticleIT {

    private static Logger logger = LoggerFactory.getLogger(HistorianVerticleIT.class);
    private static String COLLECTION = "historian";

    private com.hurence.webapiservice.historian.reactivex.HistorianService historian;
//    private ElasticsearchContainer container;
    private SolrClient client;
    private String zkUrl;

    public HistorianVerticleIT(SolrClient client, DockerComposeContainer container) {
        this.client = client;
        zkUrl = container.getServiceHost(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT)
                + ":" +
                container.getServicePort(ZOOKEEPER_SERVICE_NAME, ZOOKEEPER_PORT);
    }

    @BeforeAll
    static void beforeAll(SolrClient client) throws IOException, SolrServerException {
//        container.execInContainer()
        logger.debug("creating collection {}", COLLECTION);
//        final SolrRequest createrequest = CoreAdminRequest.createCore(COLLECTION, 1,0);
        final SolrRequest createrequest = CollectionAdminRequest.createCollection(COLLECTION, 1,1);
        client.request(createrequest);
        logger.debug("verify collection {} exist and is ready", COLLECTION);
        final SolrRequest request = CollectionAdminRequest.collectionStatus(COLLECTION);
        final NamedList<Object> rsp = client.request(request);
        final NamedList<Object> responseHeader = (NamedList<Object>) rsp.get("responseHeader");
        int status = (int) responseHeader.get("status");
        if (status != 0) {
            throw new RuntimeException(String.format("collection %s is not ready or does not exist !", COLLECTION));
        }
        logger.debug("Indexing some documents in {} collection", COLLECTION);
//        final SolrClient client = getSolrClient();
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "id1");
        doc.addField("name", "Amazon Kindle Paperwhite");
        final UpdateResponse updateResponse = client.add(COLLECTION, doc);
        client.commit(COLLECTION);
        logger.debug("Indexed some documents in {} collection", COLLECTION);
    }

    @AfterAll
    static void finish(Vertx vertx, VertxTestContext context) {
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
//        vertx.close(context.succeeding());
        assertThat(vertx.deploymentIDs())
                .isNotEmpty()
                .hasSize(1);
    }

    @Test
    @Timeout(value = 5000, timeUnit = TimeUnit.SECONDS)
    void verifyIndexState() throws Throwable {
        SolrQuery query = new SolrQuery("*:*");
        try {
            final QueryResponse response = client.query(COLLECTION, query);
            final SolrDocumentList documents = response.getResults();

            logger.info("Found " + documents.getNumFound() + " documents");
            for(SolrDocument document : documents) {
                final String id = (String) document.getFirstValue("id");
                final String name = (String) document.getFirstValue("name");

                logger.info("id: " + id + "; name: " + name);
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

