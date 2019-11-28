package com.hurence.webapiservice.http.grafana;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.historian.HistorianVerticle;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.AbstractSolrInjector;
import com.hurence.webapiservice.util.injector.SolrInjector;
import com.hurence.webapiservice.util.injector.SolrInjectorOneMetricMultipleChunksSpecificPoints;
import io.vertx.core.json.JsonObject;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@ExtendWith({VertxExtension.class, SolrExtension.class})
public class QueryEndPointFocusOnSamplingWithPreAgg {

    private static Logger LOGGER = LoggerFactory.getLogger(SearchEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        JsonObject historianConf = new JsonObject()
                //10 so if more than 5 chunk (of size 2) returned we should sample
                //with pre aggs
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_POINT, 10L)
                .put(HistorianVerticle.CONFIG_LIMIT_NUMBER_OF_CHUNK, 10000L);//we only want to test intermediate algo here
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(
                        client, container, vertx, context, historianConf);
        LOGGER.info("Indexing some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        buildInjector().injectChunks(client);
        LOGGER.info("Indexed some documents in {} collection", HistorianSolrITHelper.COLLECTION);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/query");
    }

    public static SolrInjector buildInjector() {
        List<List<Point>> pointsByChunk10Chunks = Arrays.asList(
                Arrays.asList(
                        new Point(0, 1L, 1.0),
                        new Point(0, 2L, 1.0)
                ),
                Arrays.asList(
                        new Point(0, 3L, 2.0),
                        new Point(0, 4L, 2.0)
                ),
                Arrays.asList(
                        new Point(0, 5L, 3.0),
                        new Point(0, 6L, 3.0)
                ),
                Arrays.asList(
                        new Point(0, 7L, 4.0),
                        new Point(0, 8L, 4.0)
                ),
                Arrays.asList(
                        new Point(0, 9L, 5.0),
                        new Point(0, 10L, 5.0)
                ),
                Arrays.asList(
                        new Point(0, 11L, 6.0),
                        new Point(0, 12L, 6.0)
                ),
                Arrays.asList(
                        new Point(0, 13L, 7.0),
                        new Point(0, 14L, 7.0)
                ),
                Arrays.asList(
                        new Point(0, 15L, 8.0),
                        new Point(0, 16L, 8.0)
                ),
                Arrays.asList(
                        new Point(0, 17L, 9.0),
                        new Point(0, 18L, 9.0)
                ),
                Arrays.asList(
                        new Point(0, 19L, 10.0),
                        new Point(0, 20L, 10.0)
                )
        );
        AbstractSolrInjector injector10chunk = new SolrInjectorOneMetricMultipleChunksSpecificPoints(
                "metric_10_chunk", pointsByChunk10Chunks);
        AbstractSolrInjector injector9chunk = new SolrInjectorOneMetricMultipleChunksSpecificPoints(
                "metric_9_chunk", pointsByChunk10Chunks.stream().limit(9).collect(Collectors.toList()));
        AbstractSolrInjector injector7chunk = new SolrInjectorOneMetricMultipleChunksSpecificPoints(
                "metric_7_chunk", pointsByChunk10Chunks.stream().limit(7).collect(Collectors.toList()));
        AbstractSolrInjector injector5chunk = new SolrInjectorOneMetricMultipleChunksSpecificPoints(
                "metric_5_chunk", pointsByChunk10Chunks.stream().limit(5).collect(Collectors.toList()));
        AbstractSolrInjector injector1chunkOf20Point = new SolrInjectorOneMetricMultipleChunksSpecificPoints(
                "metric_1_chunk_of_20_points",
                Arrays.asList(
                        pointsByChunk10Chunks.stream().flatMap(List::stream).collect(Collectors.toList())
                )
        );
        injector10chunk.addChunk(injector9chunk);
        injector10chunk.addChunk(injector7chunk);
        injector10chunk.addChunk(injector5chunk);
        injector10chunk.addChunk(injector1chunkOf20Point);
        return injector10chunk;
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax20(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric10ChunkMaxPoint20/request.json",
                "/http/grafana/query/preagg-algo/testMetric10ChunkMaxPoint20/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric10ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric10ChunkMaxPoint4/request.json",
                "/http/grafana/query/preagg-algo/testMetric10ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric9ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric9ChunkMaxPoint4/request.json",
                "/http/grafana/query/preagg-algo/testMetric9ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric7ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric7ChunkMaxPoint4/request.json",
                "/http/grafana/query/preagg-algo/testMetric7ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric5ChunkMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric5ChunkMaxPoint4/request.json",
                "/http/grafana/query/preagg-algo/testMetric5ChunkMaxPoint4/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testSampleMetric1ChunkOf20PointMax4Point(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFile(vertx, testContext,
                "/http/grafana/query/preagg-algo/testMetric1ChunkOf20PointMaxPoint4/request.json",
                "/http/grafana/query/preagg-algo/testMetric1ChunkOf20PointMaxPoint4/expectedResponse.json");
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertHelper.assertRequestGiveResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

}
