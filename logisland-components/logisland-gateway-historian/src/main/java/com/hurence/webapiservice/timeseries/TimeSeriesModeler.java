package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.util.ChunkUtil;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface TimeSeriesModeler {


    public static JsonArray buildTimeSeries(TimeSeriesRequest request,//TODO create an isolated interface with only needed params
                                            Map<String, List<JsonObject>> chunksByName,
                                            TimeSeriesModeler timeserieModeler) {
        List<JsonObject> timeseries = chunksByName.values().stream()
                .map(chunksOfOneMetric -> extractTimeSerie(request, timeserieModeler, chunksOfOneMetric))
                .collect(Collectors.toList());
        return new JsonArray(timeseries);
    }

    static JsonObject extractTimeSerie(TimeSeriesRequest request, TimeSeriesModeler timeserieModeler, List<JsonObject> chunksOfOneMetric) {
//        //TODO move this inside extractTimeSerieFromChunks because chunks may contain points not needed
//        SamplingConf samplingConf = request.getSamplingConf();
//        if (samplingConf.getAlgo() == SamplingAlgorithm.NONE) {//verify there is not too many point to return them all
//            long totalNumberOfPoint = ChunkUtil.countTotalNumberOfPointInChunks(chunksOfOneMetric);
//            if (totalNumberOfPoint > samplingConf.getMaxPoint()) {
//                //TODO calcul number of buckets accordingly to max point
//                int bucketSize = calculBucketSize(samplingConf.getMaxPoint(), totalNumberOfPoint);
//                samplingConf = new SamplingConf(SamplingAlgorithm.FIRST_ITEM, samplingConf.getBucketSize(), samplingConf.getMaxPoint());
//            }
//        }
        JsonObject agreggatedChunks = timeserieModeler.extractTimeSerieFromChunks(
                request.getFrom(), request.getTo(),
                request.getAggs(), request.getSamplingConf(), chunksOfOneMetric);
        return agreggatedChunks;
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
