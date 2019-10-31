package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface TimeSeriesModeler {

    public static JsonArray buildTimeSeries(long from, long to,
                                            List<AGG> aggs, SamplingConf samplingConf,
                                            Map<String, List<JsonObject>> chunksByName,
                                            TimeSeriesModeler timeserieModeler) {
        List<JsonObject> timeseries = chunksByName.values().stream()
                .map((chunksOfOneMetric) -> {
                    JsonObject agreggatedChunks = timeserieModeler.extractTimeSerieFromChunks(
                            from, to,
                            aggs, samplingConf, chunksOfOneMetric);
                    return agreggatedChunks;
                }).collect(Collectors.toList());
        return new JsonArray(timeseries);
    }
    /**
     *
     * @param from
     * @param to
     * @param aggs
     * @param samplingConf
     * @param chunks
     * @return a json object representing the result of a query on a timeserie
     *         The json content will depend on the implementation of the interface.
     */
    public JsonObject extractTimeSerieFromChunks(long from, long to, List<AGG> aggs, SamplingConf samplingConf, List<JsonObject> chunks);
}
