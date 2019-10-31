package com.hurence.webapiservice.http.grafana;

import com.hurence.logisland.record.Point;
import com.hurence.unit5.extensions.SolrExtension;
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
                        Arrays.asList(//maxDataPoints
                                new Point(0, 1477895624866L, 55),
                                new Point(0, 1477895624867L, 1767),
                                new Point(0, 1477895624868L, 861),
                                new Point(0, 1477895624869L, 767),
                                new Point(0, 1477895624870L, 44),
                                new Point(0, 1477895624871L, 767),
                                new Point(0, 1477895624872L, 3861),
                                new Point(0, 1477895624873L, 767),
                                new Point(0, 1477895624874L, -6),
                                new Point(0, 1477895624875L, 767),
                                new Point(0, 1477895624876L, 861),
                                new Point(0, 1477895624877L, -767),
                                new Point(0, 1477895624878L, 14),
                                new Point(0, 1477895624879L, 767),
                                new Point(0, 1477895624880L, 861),
                                new Point(0, 1477895624881L, 767),
                                new Point(0, 1477895624882L, 4861),
                                new Point(0, 1477895624883L, 767),
                                new Point(0, 1477895624884L, 861),
                                new Point(0, 1477895624885L, 767),
                                new Point(0, 1477895624886L, 864),
                                new Point(0, 1477895624887L, 767),
                                new Point(0, 1477895624888L, 861),
                                new Point(0, 1477895624889L, 767),
                                new Point(0, 1477895624890L, 861),
                                new Point(0, 1477895624891L, 767),
                                new Point(0, 1477895624892L, 861),
                                new Point(0, 1477895624893L, 767),
                                new Point(0, 1477895624894L, 861),
                                new Point(0, 1477895624895L, 4767),
                                new Point(0, 1477895624896L, 861),
                                new Point(0, 1477895624897L, -767),
                                new Point(0, 1477895624898L, 861),
                                new Point(0, 1477895624899L, 767),
                                new Point(0, 1477895624900L, 861),
                                new Point(0, 1477895624901L, 767),
                                new Point(0, 1477895624902L, 9861),
                                new Point(0, 1477895624903L, -767),
                                new Point(0, 1477895624904L, 861),
                                new Point(0, 1477895624905L, 7674)
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
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(getClass().getResource("/http/grafana/query/test1/request.json").getFile());
        webClient.post("/api/grafana/query")
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/grafana/query/test1/expectedResponse.json").getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testMaxDataPoints(Vertx vertx, VertxTestContext testContext) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(getClass().getResource("/http/grafana/query/testMaxDataPoints/request.json").getFile());
        webClient.post("/api/grafana/query")
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(getClass().getResource("/http/grafana/query/testMaxDataPoints/expectedResponse.json").getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }

}
