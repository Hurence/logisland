package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class GrafanaTimeSeriesModeler extends AbstractTimeSeriesModeler {
    private static String TIMESERIE_NAME = "target";
    private static String TIMESERIE_POINT = "datapoints";
    private static String TIMESERIE_AGGS = "aggs";

    /**
     *
     * @param from
     * @param to
     * @param aggs
     * @param samplingConf
     * @param chunks
     * @return a json object
     *        <pre>
     *        {
     *            {@value TIMESERIE_NAME} : "name of the metric",
     *            {@value TIMESERIE_POINT} : [
     *                                          [double, longs],
     *                                          ...,
     *                                       ]
     *        }
     *        </pre>
     */
    public JsonObject extractTimeSerieFromChunks(long from, long to, List<AGG> aggs, SamplingConf samplingConf, List<JsonObject> chunks) {
        if (chunks==null || chunks.isEmpty()) throw new IllegalArgumentException("chunks is null or empty !");
        String name = chunks.stream().findFirst().get().getString(HistorianService.METRIC_NAME);
        JsonObject timeserie = new JsonObject()
                .put(TIMESERIE_NAME, name);
        chunks = adjustChunk(from, to, aggs, chunks);
        //TODO add possibility to not get points but only aggregation if wanted
        JsonObject points = extractPointsThenSortThenSample(from, to, samplingConf, chunks);
        timeserie.mergeIn(points);
        return timeserie;
    }

    @Override
    protected JsonObject formatTimeSeriePointsJson(List<Point> sampledPoints) {
        List<JsonArray> points = sampledPoints.stream()
                .map(p -> new JsonArray().add(p.getValue()).add(p.getTimestamp()))
                .collect(Collectors.toList());
        return new JsonObject()
                .put(TIMESERIE_POINT, new JsonArray(points));
    }
}
