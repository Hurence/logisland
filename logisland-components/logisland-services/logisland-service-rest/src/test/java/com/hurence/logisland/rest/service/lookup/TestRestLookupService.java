/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.rest.service.lookup;

import com.hurence.logisland.component.InitializationException;
import com.hurence.logisland.controller.ControllerServiceInitializationContext;
import com.hurence.logisland.record.Record;
import com.hurence.logisland.service.lookup.LookupFailureException;
import com.hurence.logisland.util.runner.TestRunner;
import com.hurence.logisland.util.runner.TestRunners;

import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class TestRestLookupService {

    @Test
    public void testCustomValidate() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TestProcessor());
        MockRestLookUpService service = new MockRestLookUpService();

        //TODO add custom validate rule ?
        //should be invalid
        runner.addControllerService("restLookupService", service);
        runner.enableControllerService(service);
        runner.assertNotValid(service);
        runner.removeControllerService(service);

        // conf file with no zk properties should be valid
        service =  new MockRestLookUpService();
        runner.addControllerService("restLookupService", service);
        runner.setProperty(service, MockRestLookUpService.URL, "src/test/resources/hbase-site.xml");
        runner.enableControllerService(service);

        runner.assertValid(service);
        runner.removeControllerService(service);
    }

    @Test
    public void testSimpleCsvFileLookupService() throws InitializationException, IOException, LookupFailureException {
//        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
//        final SimpleCsvFileLookupService service = new SimpleCsvFileLookupService();
//
//        runner.addControllerService("csv-file-lookup-service", service);
//        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test.csv");
//        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "RFC4180");
//        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
//        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
//        runner.enableControllerService(service);
//        runner.assertValid(service);
//
//        final SimpleCsvFileLookupService lookupService =
//            (SimpleCsvFileLookupService) runner.getProcessContext()
//                .getControllerServiceLookup()
//                .getControllerService("csv-file-lookup-service");
//
//        assertThat(lookupService, instanceOf(LookupService.class));
//
//        final Optional<String> property1 = lookupService.lookup(Collections.singletonMap("key", "property.1"));
//        assertEquals(Optional.of("this is property 1"), property1);
//
//        final Optional<String> property2 = lookupService.lookup(Collections.singletonMap("key", "property.2"));
//        assertEquals(Optional.of("this is property 2"), property2);
//
//        final Optional<String> property3 = lookupService.lookup(Collections.singletonMap("key", "property.3"));
//        assertEquals(EMPTY_STRING, property3);
    }

    @Test
    public void testSimpleCsvFileLookupServiceWithCharset() throws InitializationException, IOException, LookupFailureException {
//        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
//        final SimpleCsvFileLookupService service = new SimpleCsvFileLookupService();
//
//        runner.addControllerService("csv-file-lookup-service", service);
//        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test_Windows-31J.csv");
//        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "RFC4180");
//        runner.setProperty(service, SimpleCsvFileLookupService.CHARSET, "Windows-31J");
//        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
//        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
//        runner.enableControllerService(service);
//        runner.assertValid(service);
//
//        final Optional<String> property1 = service.lookup(Collections.singletonMap("key", "property.1"));
//        assertThat(property1.isPresent(), is(true));
//        assertThat(property1.get(), is("this is property \uff11"));
    }


    // Override methods to create a mock service that can return staged data
    private class MockRestLookUpService extends RestLookupService {

        private Map<String, Response> fakeServer;

        public MockRestLookUpService() {
            fakeServer = new HashMap<>();
        }

        public MockRestLookUpService(final Map<String, Response> fakeServer) {
            this.fakeServer = fakeServer;
        }

        public void addServerResponse(final String url, final Response response) {
            this.fakeServer.put(url, response);
        }

        public void addServerResponse(final String url, final InputStream is) {

            final Response restResponse = Mockito.mock(Response.class);
            final ResponseBody restResponseBody = Mockito.mock(ResponseBody.class);
            //Response mock
            Mockito.when(restResponse.code()).thenReturn(404);
            Mockito.when(restResponse.body()).thenReturn(restResponseBody);
            //ResponseBody mock
            Mockito.when(restResponseBody.byteStream()).thenReturn(is);
            this.fakeServer.put(url, restResponse);
        }
    }
}
