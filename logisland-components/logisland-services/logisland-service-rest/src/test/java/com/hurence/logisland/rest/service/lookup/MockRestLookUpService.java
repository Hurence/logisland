package com.hurence.logisland.rest.service.lookup;

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
