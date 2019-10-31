package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.reactivex.HistorianService;
import com.hurence.webapiservice.historian.util.ChunkUtil;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractTimeSeriesModeler implements TimeSeriesModeler {
    protected static String TIMESERIES_TIMESTAMPS = "timestamps";
    protected static String TIMESERIES_VALUES = "values";
    protected static String TIMESERIES_AGGS_MIN = "min";
    protected static String TIMESERIES_AGGS_MAX = "max";
    protected static String TIMESERIES_AGGS_AVG = "avg";
    protected static String TIMESERIES_AGGS_COUNT = "count";
    protected static String TIMESERIES_AGGS_SUM = "sum";

    private static BinaryCompactionConverter compacter = new BinaryCompactionConverter.Builder().build();

    /**
     *
     * @param from
     * @param to
     * @param chunks
     * @return return all points uncompressing chunks
     */
    protected List<Point> extractPoints(long from, long to, List<JsonObject> chunks) {
        return extractPointsAsStream(from, to, chunks).collect(Collectors.toList());
    }

    protected Stream<Point> extractPointsAsStream(long from, long to, List<JsonObject> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getBinary(HistorianService.CHUNK_VALUE);
                    long chunkStart = chunk.getLong(HistorianService.CHUNK_START);
                    long chunkEnd = chunk.getLong(HistorianService.CHUNK_END);
                    try {
                        return compacter.unCompressPoints(binaryChunk, chunkStart, chunkEnd, from, to).stream();
                    } catch (IOException ex) {
                        throw new IllegalArgumentException("error during uncompression of a chunk !", ex);
                    }
                });
    }

    /**
     *
     * @param samplingConf how to sample points to retrieve
     * @param chunks to sample, chunks should be corresponding to the same timeserie !*
     *               Should contain the compressed binary points as well as all needed aggs.
     *               Chunks should be ordered as well.
     * @return sampled points as an array
     * <pre>
     * {
     *     {@value TIMESERIES_TIMESTAMPS} : [longs]
     *     {@value TIMESERIES_VALUES} : [doubles]
     * }
     * DOCS contains at minimum chunk_value, chunk_start
     * </pre>
     */
    protected JsonObject extractPointsThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(samplingConf.getAlgo(), samplingConf.getBucketSize());
        List<Point> sampledPoints = sampler.sample(extractPoints(from, to, chunks));
        return formatTimeSeriePointsJson(sampledPoints);
    }

    /**
     *
     * @param samplingConf how to sample points to retrieve
     * @param chunks to sample, chunks should be corresponding to the same timeserie !*
     *               Should contain the compressed binary points as well as all needed aggs.
     *               Chunks should be ordered as well.
     * @return sampled points as an array
     * <pre>
     * {
     *     {@value TIMESERIES_TIMESTAMPS} : [longs]
     *     {@value TIMESERIES_VALUES} : [doubles]
     * }
     * DOCS contains at minimum chunk_value, chunk_start
     * </pre>
     */
    protected JsonObject extractPointsThenSortThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Stream<Point> extractedPoints = extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Point::getTimestamp));
        List<Point> sampledPoints = samplePoints(samplingConf, chunks, sortedPoints);
        return formatTimeSeriePointsJson(sampledPoints);
    }

    private List<Point> samplePoints(SamplingConf samplingConf, List<JsonObject> chunks, Stream<Point> sortedPoints) {
        SamplingAlgorithm algorithm = samplingConf.getAlgo();
        int bucketSize = samplingConf.getBucketSize();
        if (samplingConf.getAlgo() == SamplingAlgorithm.NONE) {//verify there is not too many point to return them all
            int totalNumberOfPoint = ChunkUtil.countTotalNumberOfPointInChunks(chunks);
            if (totalNumberOfPoint > samplingConf.getMaxPoint()) {
                algorithm = SamplingAlgorithm.FIRST_ITEM;
                bucketSize = calculBucketSize(samplingConf.getMaxPoint(), totalNumberOfPoint);
            }
        }
        Sampler<Point> sampler = SamplerFactory.getPointSampler(algorithm, bucketSize);
        return sampler.sample(sortedPoints.collect(Collectors.toList()));
    }

    private int calculBucketSize(int maxPoint, int totalNumberOfPoint) {
           return Math.floorDiv(totalNumberOfPoint, maxPoint);
    }

    protected JsonObject formatTimeSeriePointsJson(List<Point> sampledPoints) {
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

    /**
     * @param from minimum timestamp for points in chunks.
     *             If a chunk contains a point with a lower timestamp it will be recalculated without those points
     * @param to maximum timestamp for points in chunks.
     *           If a chunk contains a point with a higher timestamp it will be recalculated without those points
     * @param aggs the desired aggs for the chunk, it will be recalculated if needed
     * @param chunks the current chunks
     * @return chunks if no points are out of bound otherwise return chunks with recalculated ones.
     */
    protected List<JsonObject> adjustChunk(long from, long to, List<AGG> aggs, List<JsonObject> chunks) {
        //TODO only necessary if we want to optimize aggs. But if we have to recompute all chunks this may not be really helpfull.
        //TODO depending on the size of the bucket desired.

        return chunks;
    }
}
