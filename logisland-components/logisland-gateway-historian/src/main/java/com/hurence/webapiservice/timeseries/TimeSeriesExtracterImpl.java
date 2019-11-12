package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.webapiservice.historian.HistorianFields;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesExtracterImpl implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    private static String TIMESERIE_NAME = "target";
    private static String TIMESERIE_POINT = "datapoints";
    private static String TIMESERIE_AGGS = "aggs";
    private final long from;
    private final long to;
    private final SamplingConf samplingConf;
    private final Sampler<Point> sampler;
    private final String metricName;
    private final List<JsonObject> chunks = new ArrayList<>();
    private final List<Point> sampledPoints = new ArrayList<>();
    private long totalChunkCounter = 0L;
    private long toatlPointCounter = 0L;
    private long pointCounter = 0L;

    public TimeSeriesExtracterImpl(String metricName, long from, long to, SamplingConf samplingConf, long totalNumberOfPoint) {
        this.metricName = metricName;
        this.samplingConf = samplingConf;
        this.from = from;
        this.to = to;
        sampler = TimeSeriesExtracterUtil.getPointSampler(samplingConf, totalNumberOfPoint);
    }

    @Override
    public void addChunk(JsonObject chunk) {
        totalChunkCounter++;
        pointCounter+=chunk.getLong(HistorianFields.RESPONSE_CHUNK_SIZE_FIELD);
        chunks.add(chunk);
        if (pointCounter >= samplingConf.getBucketSize()) {//TODO
            samplePointsInBufferThenReset();
        }
    }

    @Override
    public void flush() {
        samplePointsInBufferThenReset();
    }

    private void samplePointsInBufferThenReset() {
        Stream<Point> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        //TODO Do we want to sort Points here ?
        List<Point> sampledPoints = sampler.sample(extractedPoints.collect(Collectors.toList()));
        this.sampledPoints.addAll(sampledPoints);
        chunks.clear();
        toatlPointCounter+=pointCounter;
        pointCounter = 0;
    }

    @Override
    public JsonObject getTimeSeries() {
        List<JsonArray> points = sampledPoints.stream()
                .map(p -> new JsonArray().add(p.getValue()).add(p.getTimestamp()))
                .collect(Collectors.toList());
        JsonObject toReturn = new JsonObject()
                .put(TIMESERIE_NAME, metricName)
                .put(TIMESERIE_POINT, new JsonArray(points));
        LOGGER.trace("getTimeSeries return : {}", toReturn.encodePrettily());
        return toReturn;
    }

    @Override
    public long chunkCount() {
        return totalChunkCounter;
    }

    @Override
    public long pointCount() {
        return toatlPointCounter;
    }
}
