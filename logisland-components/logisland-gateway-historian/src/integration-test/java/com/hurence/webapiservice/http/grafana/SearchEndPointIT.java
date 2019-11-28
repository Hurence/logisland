package com.hurence.webapiservice.http.grafana;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjector;
import com.hurence.webapiservice.util.injector.SolrInjectorDifferentMetricNames;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class SearchEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjector injector = new SolrInjectorDifferentMetricNames(3, 2);
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        webClient = HttpITHelper.buildWebClient(vertx);
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSearch(Vertx vertx, VertxTestContext testContext) {
        webClient.post("/api/grafana/search")
                .as(BodyCodec.jsonArray())
                .send(testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        FileSystem fs = vertx.fileSystem();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/grafana/search/test1/expectedResponse.json").getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
}
