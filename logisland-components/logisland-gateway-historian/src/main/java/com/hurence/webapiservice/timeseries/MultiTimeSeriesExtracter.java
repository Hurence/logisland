package com.hurence.webapiservice.timeseries;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface MultiTimeSeriesExtracter {

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonArray getTimeSeries();

    long chunkCount();

    long pointCount();

}
