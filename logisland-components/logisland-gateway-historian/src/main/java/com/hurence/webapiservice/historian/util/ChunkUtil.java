package com.hurence.webapiservice.historian.util;

import com.hurence.webapiservice.historian.HistorianFields;
import com.hurence.webapiservice.historian.HistorianService;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class ChunkUtil {

    private ChunkUtil() {}

    public static int countTotalNumberOfPointInChunks(List<JsonObject> chunks) throws UnsupportedOperationException {
        return chunks.stream()
                .mapToInt(chunk -> chunk.getInteger(HistorianFields.RESPONSE_CHUNK_SIZE_FIELD))
                .sum();
    }
}
