/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.rest.service.lookup;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.record.RecordUtils;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.service.lookup.RecordLookupService;
import com.hurence.logisland.util.runner.MockRecord;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TestRestLookupService {

    @Test
    public void testCustomValidate() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        MockRestLookUpService service = new MockRestLookUpService();

        //TODO add custom validate rule ?
        //should be invalid
        runner.addControllerService("restLookupService", service);
        runner.assertNotValid(service);

        // conf file with no zk properties should be valid
        runner.addControllerService("restLookupService", service);
        runner.setProperty(service, MockRestLookUpService.URL, "src/test/resources/hbase-site.xml");
        runner.assertValid(service);
        runner.enableControllerService(service);
        runner.removeControllerService(service);
    }

    @Test
    public void testStaticRestLookupService() throws InitializationException, IOException, LookupFailureException {
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        MockRestLookUpService service = new MockRestLookUpService();
        //build mock urls
        service.addServerResponse("http://hurence.com/employee/1",
                "{ \"name\" : \"greg\" }".getBytes(StandardCharsets.UTF_8));
        service.addServerResponse("http://hurence.com/employee/2",
                "{ \"name\" : \"jésus\" }".getBytes(StandardCharsets.UTF_8));
        //enable service
        runner.addControllerService("restLookupService", service);
        runner.setProperty(service, MockRestLookUpService.URL, "http://hurence.com/employee/1");
        runner.enableControllerService(service);
        runner.assertValid(service);
        //test queries
        final RecordLookupService lookupService = (RecordLookupService) runner.getControllerService("restLookupService");
        assertThat(lookupService, instanceOf(RestLookupService.class));

        MockRecord record1 = new MockRecord(lookupService.lookup(Collections.emptyMap()).get());
        record1.assertFieldExists("name");
        record1.assertFieldEquals("name", "greg");
        record1.assertRecordSizeEquals(1);

        runner.disableControllerService(service);
        runner.setProperty(service, MockRestLookUpService.URL, "http://hurence.com/employee/2");
        runner.enableControllerService(service);

        MockRecord record2 = new MockRecord(lookupService.lookup(Collections.emptyMap()).get());
        record2.assertFieldExists("name");
        record2.assertFieldEquals("name", "jésus");
        record2.assertRecordSizeEquals(1);
    }

    @Test
    public void testDynamicRestLookupService() throws InitializationException, IOException, LookupFailureException {
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        MockRestLookUpService service = new MockRestLookUpService();
        //build mock urls
        service.addServerResponse("http://hurence.com/ressource1/id1",
                "{ \"name\" : \"greg\" }".getBytes(StandardCharsets.UTF_8));
        service.addServerResponse("http://hurence.com/ressource2/id2",
                "{ \"name\" : \"jésus\" }".getBytes(StandardCharsets.UTF_8));
        //enable service
        runner.addControllerService("restLookupService", service);
        runner.setProperty(service, MockRestLookUpService.URL, "http://hurence.com/${ressource}/${id}");
        runner.enableControllerService(service);
        runner.assertValid(service);

        runner.setProperty(TestProcessor.LOOKUP_SERVICE, "restLookupService");

        runner.enqueue(RecordUtils.getRecordOfString(
                "ressource", "ressource1",
                "id", "id1"));
        runner.enqueue(RecordUtils.getRecordOfString(
                "ressource", "ressource2",
                "id", "id2"));

        //test queries
        runner.run();
        runner.assertAllInputRecordsProcessed();

        final MockRecord outputRecord1 = runner.getOutputRecords().get(0);
        outputRecord1.assertFieldExists("name");
        outputRecord1.assertFieldEquals("name", "greg");
        outputRecord1.assertRecordSizeEquals(1);

        final MockRecord outputRecord2 = runner.getOutputRecords().get(1);
        outputRecord2.assertFieldExists("name");
        outputRecord2.assertFieldEquals("name", "jésus");
        outputRecord2.assertRecordSizeEquals(1);
    }

    //TODO test with a proxy
    //TODO test with SSL


    // Override methods to create a mock service that can return staged data
    private class MockRestLookUpService extends RestLookupService {

        //matching a url to a payload
        private Map<String, byte[]> fakeServer;

        public MockRestLookUpService() {
            fakeServer = new HashMap<>();
        }

        public MockRestLookUpService(final Map<String, byte[]> fakeServer) {
            this.fakeServer = fakeServer;
        }

        public void addServerResponse(final String url, final byte[] payload) {
            this.fakeServer.put(url, payload);
        }

        @Override
        protected Response executeRequest(Request request) throws IOException {
            super.executeRequest(request);
            String url = request.url().toString();
            if (!fakeServer.containsKey(url)) {
                throw new IllegalArgumentException(String.format("Please add a fake payload for url : '%s'", url));
            }
            byte[] payload = fakeServer.get(url);

            //mock Source
            final BufferedSource source = Mockito.mock(BufferedSource.class);
            Mockito.when(source.inputStream())
                    .thenReturn(new ByteArrayInputStream(payload));
            //ResponseBody mock
            final ResponseBody restResponseBody = Mockito.mock(ResponseBody.class);
            Mockito.when(restResponseBody.source()).thenReturn(source);

            return new Response.Builder()
                    .request(request)
                    .body(restResponseBody)
                    .code(200)
                    .protocol(Protocol.HTTP_2)
                    .message("ok")
                    .build();
        }
    }
}
