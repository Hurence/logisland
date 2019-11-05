package com.hurence.webapiservice.http.grafana;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjector;
import com.hurence.webapiservice.util.injector.SolrInjectorMultipleMetricSpecificPoints;
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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

//import io.vertx.ext.web.client.WebClient;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        SolrInjector injector = new SolrInjectorMultipleMetricSpecificPoints(
                Arrays.asList("temp_a", "temp_b", "maxDataPoints"),
                Arrays.asList(
                        Arrays.asList(
                                new Point(0, 1477895624866L, 622),
                                new Point(0, 1477916224866L, -3),
                                new Point(0, 1477917224866L, 365)
                        ),
                        Arrays.asList(
                                new Point(0, 1477895624866L, 861),
                                new Point(0, 1477917224866L, 767)
                        ),
                        Arrays.asList(//maxDataPoints we are not testing value only sampling
                                new Point(0, 1477895624866L, 1),
                                new Point(0, 1477895624867L, 1),
                                new Point(0, 1477895624868L, 1),
                                new Point(0, 1477895624869L, 1),
                                new Point(0, 1477895624870L, 1),
                                new Point(0, 1477895624871L, 1),
                                new Point(0, 1477895624872L, 1),
                                new Point(0, 1477895624873L, 1),
                                new Point(0, 1477895624874L, 1),
                                new Point(0, 1477895624875L, 1),
                                new Point(0, 1477895624876L, 1),
                                new Point(0, 1477895624877L, 1),
                                new Point(0, 1477895624878L, 1),
                                new Point(0, 1477895624879L, 1),
                                new Point(0, 1477895624880L, 1),
                                new Point(0, 1477895624881L, 1),
                                new Point(0, 1477895624882L, 1),
                                new Point(0, 1477895624883L, 1),
                                new Point(0, 1477895624884L, 1),
                                new Point(0, 1477895624885L, 1),
                                new Point(0, 1477895624886L, 1),
                                new Point(0, 1477895624887L, 1),
                                new Point(0, 1477895624888L, 1),
                                new Point(0, 1477895624889L, 1),
                                new Point(0, 1477895624890L, 1),
                                new Point(0, 1477895624891L, 1),
                                new Point(0, 1477895624892L, 1),
                                new Point(0, 1477895624893L, 1),
                                new Point(0, 1477895624894L, 1),
                                new Point(0, 1477895624895L, 1),
                                new Point(0, 1477895624896L, 1),
                                new Point(0, 1477895624897L, 1),
                                new Point(0, 1477895624898L, 1),
                                new Point(0, 1477895624899L, 1),
                                new Point(0, 1477895624900L, 1),
                                new Point(0, 1477895624901L, 1),
                                new Point(0, 1477895624902L, 1),
                                new Point(0, 1477895624903L, 1),
                                new Point(0, 1477895624904L, 1),
                                new Point(0, 1477895624905L, 1)
                        )
                ));
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
    public void testQuery(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/test1/request.json",
                "/http/grafana/query/test1/expectedResponse.json");
    }

    //TODO use parametric tests so that we can add new tests by adding files without touching code
    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints5(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax5/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax5/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints6(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax6/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax6/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints7(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax7/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax7/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints8(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax8/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax8/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints9(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax9/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax9/expectedResponse.json");
    }


    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints10(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax10/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax10/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints15(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testMaxDataPoints/testMax15/request.json",
                "/http/grafana/query/testMaxDataPoints/testMax15/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageDefaultBucket(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testWithAlgo/average/default-bucket/request.json",
                "/http/grafana/query/testWithAlgo/average/default-bucket/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize2(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testWithAlgo/average/bucket-2/request.json",
                "/http/grafana/query/testWithAlgo/average/bucket-2/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testAlgoAverageBucketSize3(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/testWithAlgo/average/bucket-3/request.json",
                "/http/grafana/query/testWithAlgo/average/bucket-3/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        AssertResponseGivenRequestHelper.assertRequestGiveResponseFromFile(webClient, "/api/grafana/query",
                vertx, testContext, requestFile, responseFile);
    }

}
