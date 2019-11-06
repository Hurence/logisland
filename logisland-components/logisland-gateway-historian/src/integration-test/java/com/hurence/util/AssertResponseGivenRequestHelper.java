package com.hurence.util;

import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.FileSystem;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertResponseGivenRequestHelper {

    private WebClient webClient;
    private String endpoint;

    public AssertResponseGivenRequestHelper(WebClient webClient, String endpoint) {
        this.webClient = webClient;
        this.endpoint = endpoint;
    }

    public void assertRequestGiveResponseFromFile(Vertx vertx, VertxTestContext testContext,
                                                         String requestFile, String responseFile) {
        final FileSystem fs = vertx.fileSystem();
        Buffer requestBuffer = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(requestFile).getFile());
        webClient.post(endpoint)
                .as(BodyCodec.jsonArray())
                .sendBuffer(requestBuffer.getDelegate(), testContext.succeeding(rsp -> {
                    testContext.verify(() -> {
                        assertEquals(200, rsp.statusCode());
                        assertEquals("OK", rsp.statusMessage());
                        JsonArray body = rsp.body();
                        Buffer fileContent = fs.readFileBlocking(AssertResponseGivenRequestHelper.class.getResource(responseFile).getFile());
                        JsonArray expectedBody = new JsonArray(fileContent.getDelegate());
                        assertEquals(expectedBody, body);
                        testContext.completeNow();
                    });
                }));
    }
}
