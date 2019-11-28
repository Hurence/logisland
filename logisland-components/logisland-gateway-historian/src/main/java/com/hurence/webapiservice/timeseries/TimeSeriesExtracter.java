package com.hurence.webapiservice.timeseries;

import io.vertx.core.json.JsonObject;

public interface TimeSeriesExtracter {

    void addChunk(JsonObject chunk);

    /**
     * Sample left chunks
     */
    void flush();

    JsonObject getTimeSeries();

    long chunkCount();

    long pointCount();
}
