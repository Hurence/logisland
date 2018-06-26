/*
 *  * Copyright (C) 2018 Hurence (support@hurence.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.engine.spark.remote;

import okhttp3.Credentials;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RemoteApiClientTest {

    private final String dataflowName = "dummy";

    private RemoteApiClient createInstance(MockWebServer server, String user, String password) {
        return new RemoteApiClient(new RemoteApiClient.ConnectionSettings( server.url("/").toString(),
                Duration.ofSeconds(2), Duration.ofSeconds(2), user, password));
    }

    @Test
    public void testAllUnsecured() throws Exception {

        try (MockWebServer mockWebServer = new MockWebServer()) {
            mockWebServer.enqueue(new MockResponse().setResponseCode(404));
            mockWebServer.enqueue(new MockResponse().setBodyDelay(3, TimeUnit.SECONDS));
            final String dummy = "\"name\":\"myName\", \"component\":\"myComponent\"";
            mockWebServer.enqueue(new MockResponse().setBody("{" + dummy + ",\"lastModified\":\"1983-06-04T10:01:02Z\"," +
                    "\"streams\":[{" + dummy + "}]}"));
            RemoteApiClient client = createInstance(mockWebServer, null, null);
            Assert.assertFalse(client.fetchDataflow(dataflowName, new RemoteApiClient.State()).isPresent());
            Assert.assertFalse(client.fetchDataflow(dataflowName, new RemoteApiClient.State()).isPresent());
            Assert.assertTrue(client.fetchDataflow(dataflowName, new RemoteApiClient.State()).isPresent());

        }


    }

    @Test
    public void testValidationFails() throws Exception {
        try (MockWebServer mockWebServer = new MockWebServer()) {
            mockWebServer.enqueue(new MockResponse().setBody("{\"name\":\"divPo\", \"lastModified\":\"1983-06-04T10:01:02Z\",\"services\":[{}],\"streams\":[{}]}"));
            RemoteApiClient client = createInstance(mockWebServer, null, null);
            Assert.assertFalse(client.fetchDataflow(dataflowName, new RemoteApiClient.State()).isPresent());
        }


    }

    @Test
    public void testAuthentication() throws Exception {
        try (MockWebServer mockWebServer = new MockWebServer()) {
            RemoteApiClient client = createInstance(mockWebServer, "test", "test");
            mockWebServer.enqueue(new MockResponse().setBody("{}"));
            client.fetchDataflow(dataflowName, new RemoteApiClient.State());
            RecordedRequest request = mockWebServer.takeRequest();
            String auth = request.getHeader(HttpHeaders.AUTHORIZATION);
            Assert.assertEquals(Credentials.basic("test", "test"), auth);
        }
    }

    @Test
    public void testUri() throws Exception {
        try (MockWebServer mockWebServer = new MockWebServer()) {
            RemoteApiClient client = createInstance(mockWebServer, null, null);
            mockWebServer.enqueue(new MockResponse().setBody("{}"));
            client.fetchDataflow(dataflowName, new RemoteApiClient.State());
            RecordedRequest request = mockWebServer.takeRequest();
            Assert.assertEquals("/dataflows/"+dataflowName, request.getPath());
        }
    }
}
