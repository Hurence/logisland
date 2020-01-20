package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.http.grafana.GrafanaApiImpl;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public class GrafanaTimeSeriesModeler implements TimeSeriesModeler {
    private static String TIMESERIE_NAME = "target";
    private static String TIMESERIE_POINT = "datapoints";
    private static String TIMESERIE_NUM_POINTS = "datapoints_count";
    private static String TIMESERIE_AGGS = "aggs";
    private static final Logger LOGGER = LoggerFactory.getLogger(GrafanaTimeSeriesModeler.class);

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
        String name = chunks.stream().findFirst().get().getString(RESPONSE_METRIC_NAME_FIELD);
        List<JsonArray> points = getPoints(from, to, samplingConf, chunks);
        return new JsonObject()
                .put(TIMESERIE_NUM_POINTS, points.size())
                .put(TIMESERIE_NAME, name)
                .put(TIMESERIE_POINT, new JsonArray(points));
    }

    public List<JsonArray> getPoints(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        List<Point> sampledPoints = TimeSeriesExtracterUtil.extractPointsThenSortThenSample(from, to, samplingConf, chunks);
        return sampledPoints.stream()
                .map(p -> new JsonArray().add(p.getValue()).add(p.getTimestamp()))
                .collect(Collectors.toList());
    }
}
