package com.hurence.webapiservice.http.grafana;

import com.hurence.unit5.extensions.SolrExtension;
import com.hurence.util.AssertResponseGivenRequestHelper;
import com.hurence.webapiservice.util.HttpITHelper;
import com.hurence.webapiservice.util.HttpWithHistorianSolrITHelper;
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
public class TagKeysAndValuesEndPointIT {

    private static Logger LOGGER = LoggerFactory.getLogger(TagKeysAndValuesEndPointIT.class);
    private static WebClient webClient;
    private static AssertResponseGivenRequestHelper assertTagKeyHelper;
    private static AssertResponseGivenRequestHelper assertTagValueHelper;

    @BeforeAll
    public static void beforeAll(SolrClient client, DockerComposeContainer container, Vertx vertx, VertxTestContext context) throws InterruptedException, IOException, SolrServerException {
        HttpWithHistorianSolrITHelper
                .initWebClientAndHistorianSolrCollectionAndHttpVerticleAndHistorianVerticle(client, container, vertx, context);
        webClient = HttpITHelper.buildWebClient(vertx);
        assertTagKeyHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/tag-keys");
        assertTagValueHelper = new AssertResponseGivenRequestHelper(webClient, "/api/grafana/tag-values");
    }

    @AfterAll
    public static void afterAll(Vertx vertx, VertxTestContext context) {
        webClient.close();
        vertx.close(context.succeeding(rsp -> context.completeNow()));
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testTagKey(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFileTagKeys(vertx, testContext,
                "/http/grafana/tagkeys/test1/request.json",
                "/http/grafana/tagkeys/test1/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testTagValuesAlgo(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFileTagValues(vertx, testContext,
                "/http/grafana/tagvalues/testAlgo/request.json",
                "/http/grafana/tagvalues/testAlgo/expectedResponse.json");
    }

    @Test
    @Timeout(value = 5, timeUnit = TimeUnit.SECONDS)
    public void testTagValuesBucketSize(Vertx vertx, VertxTestContext testContext) {
        assertRequestGiveResponseFromFileTagValues(vertx, testContext,
                "/http/grafana/tagvalues/testBucketSize/request.json",
                "/http/grafana/tagvalues/testBucketSize/expectedResponse.json");
    }



    public void assertRequestGiveResponseFromFileTagKeys(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertTagKeyHelper.assertRequestGiveResponseFromFile(vertx, testContext, requestFile, responseFile);
    }

    public void assertRequestGiveResponseFromFileTagValues(Vertx vertx, VertxTestContext testContext,
                                                  String requestFile, String responseFile) {
        assertTagValueHelper.assertRequestGiveResponseFromFile(vertx, testContext, requestFile, responseFile);
    }
}
