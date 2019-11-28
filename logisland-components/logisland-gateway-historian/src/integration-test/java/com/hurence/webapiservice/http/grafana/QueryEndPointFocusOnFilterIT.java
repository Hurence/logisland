package com.hurence.webapiservice.http.grafana;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjector;
import com.hurence.webapiservice.util.injector.SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

//import io.vertx.ext.web.client.WebClient;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnFilterIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjector injector = new SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(
                "metric_to_filter",
                Arrays.asList(
                        Arrays.asList("Berlin"),
                        Arrays.asList("France", "Berlin"),
                        Arrays.asList("usine_1"),
                        Arrays.asList("France")
                ),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 1477895624866L, 1.0),
                                new Point(0, 1477916224866L, 1.0),
                                new Point(0, 1477917224866L, 1.0)
                        ),
                        Arrays.asList(
                                new Point(0, 1477917224868L, 2.0),
                                new Point(0, 1477917224886L, 2.0)
                        ),
                        Arrays.asList(
                                new Point(0, 1477917224980L, 3.0),
                                new Point(0, 1477917224981L, 3.0)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new Point(0, 1477917224988L, 4.0),
                                new Point(0, 1477917324988L, 4.0)
                        )
                ));
        injector.injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/query");
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsBerlin(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/berlin/request.json",
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/berlin/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsFrance(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/france/request.json",
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/france/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testFilterOnTagsBerlinAndFrance(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/franceAndBerlin/request.json",
                "/http/grafana/query/extract-algo/testWithAdhocFilters/testFilterOnTags/franceAndBerlin/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
