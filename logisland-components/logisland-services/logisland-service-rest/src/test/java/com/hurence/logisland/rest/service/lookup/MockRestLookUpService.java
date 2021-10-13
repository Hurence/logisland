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

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

// Override methods to create a mock service that can return staged data
public class MockRestLookUpService extends RestLookupService {

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
        String url = request.url().toString();
        if (!fakeServer.containsKey(url)) {
            throw new IllegalArgumentException(String.format("Please add a fake payload for url : '%s'", url));
        }
        byte[] payload = fakeServer.get(url);

        //mock Source
        final BufferedSource source = Mockito.mock(BufferedSource.class);
        Mockito.when(source.inputStream())
                .thenReturn(new ByteArrayInputStream(payload));
        Mockito.when(source.buffer())
                .thenReturn(new Buffer().write(payload));
        Mockito.when(source.readString(any()))
                .thenReturn(new String(payload));

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
