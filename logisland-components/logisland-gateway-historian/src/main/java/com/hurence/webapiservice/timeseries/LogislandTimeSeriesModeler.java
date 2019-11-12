package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

import static com.hurence.webapiservice.historian.HistorianFields.RESPONSE_METRIC_NAME_FIELD;
public class LogislandTimeSeriesModeler implements TimeSeriesModeler {
    private static String TIMESERIES_NAME = "name";
    private static String TIMESERIES_AGGS = "aggs";
    private static String TIMESERIES_TIMESTAMPS = TimeSeriesExtracterUtil.TIMESERIES_TIMESTAMPS;
    private static String TIMESERIES_VALUES = TimeSeriesExtracterUtil.TIMESERIES_VALUES;
    private static String TIMESERIES_AGGS_MIN = "min";
    private static String TIMESERIES_AGGS_MAX = "max";
    private static String TIMESERIES_AGGS_AVG = "avg";
    private static String TIMESERIES_AGGS_COUNT = "count";
    private static String TIMESERIES_AGGS_SUM = "sum";

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
     *            {@value TIMESERIES_NAME} : "name of the metric",
     *            {@value TIMESERIES_TIMESTAMPS} : [longs]
     *            {@value TIMESERIES_VALUES} : [doubles]
     *            {@value TIMESERIES_AGGS} : "total chunk matching query"
     *        }
     *        DOCS contains at minimum chunk_value, chunk_start
     *        </pre>
     */
    public JsonObject extractTimeSerieFromChunks(long from, long to, List<AGG> aggs, SamplingConf samplingConf, List<JsonObject> chunks) {
        if (chunks==null || chunks.isEmpty()) throw new IllegalArgumentException("chunks is null or empty !");
        String name = chunks.stream().findFirst().get().getString(RESPONSE_METRIC_NAME_FIELD);
        JsonObject timeserie = new JsonObject()
                .put(TIMESERIES_NAME, name);
        JsonObject points = getPoints(from, to, samplingConf, chunks);
        timeserie.mergeIn(points);
        JsonObject timeSeriesAggs = calculAggs(aggs, chunks);
        if (!timeSeriesAggs.isEmpty())
            timeserie.put(TIMESERIES_AGGS, timeSeriesAggs);
        return timeserie;

    }

    private JsonObject getPoints(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        List<Point> sampledPoints = TimeSeriesExtracterUtil.extractPointsThenSortThenSample(from, to, samplingConf, chunks);
        List<Long> timestamps = sampledPoints.stream()
                .map(Point::getTimestamp)
                .collect(Collectors.toList());
        List<Double> values = sampledPoints.stream()
                .map(Point::getValue)
                .collect(Collectors.toList());
        return new JsonObject()
                .put(TIMESERIES_TIMESTAMPS, timestamps)
                .put(TIMESERIES_VALUES, values);
    }

    /**
     *
     * @param aggs to calculate
     * @param chunks to use for calculation, each chunk should already contain needed agg. The chunk should
     *               reference the same timeserie.
     * @return a JsonObject containing aggregation over all chunks
     */
    protected JsonObject calculAggs(List<AGG> aggs, List<JsonObject> chunks) {
        final JsonObject aggsJson = new JsonObject();
        aggs.forEach(agg -> {
            //TODO find a way to not use a switch, indeed this would imply to modify this code every time we add an agg
            //TODO instead for example use an interface ? And a factory ?
            final String aggField;
            switch (agg) {
                case MIN:
                    //TODO
                    aggsJson.put(TIMESERIES_AGGS_MIN, "TODO");
                    break;
                case MAX:
                    aggsJson.put(TIMESERIES_AGGS_MAX, "TODO");
                    break;
                case AVG:
                    aggsJson.put(TIMESERIES_AGGS_AVG, "TODO");
                    break;
                case COUNT:
                    aggsJson.put(TIMESERIES_AGGS_COUNT, "TODO");
                    break;
                case SUM:
                    aggsJson.put(TIMESERIES_AGGS_SUM, "TODO");
                    break;
                default:
                    throw new IllegalStateException("Unsupported aggregation: " + agg);
            }
        });
        return aggsJson;
    }

}
