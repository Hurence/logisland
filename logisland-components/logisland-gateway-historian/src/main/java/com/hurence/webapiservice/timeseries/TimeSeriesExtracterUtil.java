package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.compaction.BinaryCompactionConverter;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.logisland.timeseries.sampling.SamplingAlgorithm;
import com.hurence.webapiservice.historian.util.ChunkUtil;
import com.hurence.webapiservice.modele.AGG;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public class TimeSeriesExtracterUtil {
    public final static String TIMESERIES_TIMESTAMPS = "timestamps";
    public final static String TIMESERIES_VALUES = "values";
    public final static SamplingAlgorithm DEFAULT_SAMPLING_ALGORITHM = SamplingAlgorithm.FIRST_ITEM;


    private TimeSeriesExtracterUtil() {}

    private static BinaryCompactionConverter compacter = new BinaryCompactionConverter.Builder().build();
    /**
     *
     * @param from
     * @param to
     * @param chunks
     * @return return all points uncompressing chunks
     */
    public static List<Point> extractPoints(long from, long to, List<JsonObject> chunks) {
        return extractPointsAsStream(from, to, chunks).collect(Collectors.toList());
    }

    public static Stream<Point> extractPointsAsStream(long from, long to, List<JsonObject> chunks) {
        return chunks.stream()
                .flatMap(chunk -> {
                    byte[] binaryChunk = chunk.getBinary(RESPONSE_CHUNK_VALUE_FIELD);
                    long chunkStart = chunk.getLong(RESPONSE_CHUNK_START_FIELD);
                    long chunkEnd = chunk.getLong(RESPONSE_CHUNK_END_FIELD);
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
    public static List<Point> extractPointsThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Sampler<Point> sampler = SamplerFactory.getPointSampler(samplingConf.getAlgo(), samplingConf.getBucketSize());
        return sampler.sample(extractPoints(from, to, chunks));
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
    public static List<Point> extractPointsThenSortThenSample(long from, long to, SamplingConf samplingConf, List<JsonObject> chunks) {
        Stream<Point> extractedPoints = extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Point::getTimestamp));
        return samplePoints(samplingConf, chunks, sortedPoints);
    }

    public static List<Point> samplePoints(SamplingConf samplingConf, List<JsonObject> chunks, Stream<Point> sortedPoints) {
        int totalNumberOfPoint = ChunkUtil.countTotalNumberOfPointInChunks(chunks);
        Sampler<Point> sampler = getPointSampler(samplingConf, totalNumberOfPoint);
        return sampler.sample(sortedPoints.collect(Collectors.toList()));
    }

    public static Sampler<Point> getPointSampler(SamplingConf samplingConf, long totalNumberOfPoint) {
        SamplingConf calculatedConf = calculSamplingConf(samplingConf, totalNumberOfPoint);
        return SamplerFactory.getPointSampler(calculatedConf.getAlgo(), calculatedConf.getBucketSize());
    }

    public static SamplingConf calculSamplingConf(SamplingConf samplingConf, long totalNumberOfPoint) {
        SamplingAlgorithm algorithm = calculSamplingAlgorithm(samplingConf, totalNumberOfPoint);
        int bucketSize = samplingConf.getBucketSize();
        if (totalNumberOfPoint > samplingConf.getMaxPoint()) {
            //verify there is not too many point to return them all otherwise recalcul bucket size accordingly.
            bucketSize = calculBucketSize(samplingConf.getMaxPoint(), totalNumberOfPoint);
        }
        return new SamplingConf(algorithm, bucketSize, samplingConf.getMaxPoint());
    }

    public static SamplingAlgorithm calculSamplingAlgorithm(SamplingConf samplingConf, long totalNumberOfPoint) {
        if (samplingConf.getAlgo() == SamplingAlgorithm.NONE && totalNumberOfPoint > samplingConf.getMaxPoint())
            return DEFAULT_SAMPLING_ALGORITHM;
        return samplingConf.getAlgo();
    }


    private static int calculBucketSize(int maxPoint, int totalNumberOfPoint) {
        return BucketUtils.calculBucketSize(totalNumberOfPoint, maxPoint);
    }

    private static int calculBucketSize(int maxPoint, long totalNumberOfPoint) {
        return BucketUtils.calculBucketSize(totalNumberOfPoint, maxPoint);
    }


    public static JsonObject formatTimeSeriePointsJson(List<Point> sampledPoints) {
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
}
