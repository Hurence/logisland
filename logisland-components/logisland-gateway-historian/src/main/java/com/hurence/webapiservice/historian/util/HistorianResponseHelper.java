package com.hurence.webapiservice.historian.util;

import com.hurence.webapiservice.historian.HistorianFields;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class HistorianResponseHelper {

    private HistorianResponseHelper() {}

    public static List<JsonObject> extractChunks(JsonObject chunkResponse) throws UnsupportedOperationException {
        final long totalFound = chunkResponse.getLong(HistorianFields.RESPONSE_TOTAL_FOUND);
        List<JsonObject> chunks = chunkResponse.getJsonArray(HistorianFields.RESPONSE_CHUNKS).stream()
                .map(JsonObject.class::cast)
                .collect(Collectors.toList());
        if (totalFound != chunks.size())
            //TODO add a test with more than 10 chunks then implement handling more than default 10 chunks of solr
            //TODO should we add initial number of chunk to fetch in query param ?
            throw new UnsupportedOperationException("not yet supported when matching more than "+
                    chunks.size() + " chunks (total found : " + totalFound +")");
        return chunks;
    }


}
