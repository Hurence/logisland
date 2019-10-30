package com.hurence.webapiservice.timeseries;

import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class LogislandTimeSeriesModeler extends AbstractTimeSeriesModeler {
    private static String TIMESERIES_NAME = "name";
    private static String TIMESERIES_AGGS = "aggs";

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
        String name = chunks.stream().findFirst().get().getString(HistorianService.METRIC_NAME);
        JsonObject timeserie = new JsonObject()
                .put(TIMESERIES_NAME, name);
        chunks = adjustChunk(from, to, aggs, chunks);
        //TODO add possibility to not get points but only aggregation if wanted
        JsonObject points = samplePoints(from, to, samplingConf, chunks);
        timeserie.mergeIn(points);
        JsonObject timeSeriesAggs = calculAggs(aggs, chunks);
        if (!timeSeriesAggs.isEmpty())
            timeserie.put(TIMESERIES_AGGS, timeSeriesAggs);
        return timeserie;
    }
}
