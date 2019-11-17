package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.historian.HistorianFields;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesExtracterImpl implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    private static String TIMESERIE_NAME = "target";
    private static String TIMESERIE_POINT = "datapoints";
    private static String TIMESERIE_AGGS = "aggs";
    final long from;
    final long to;
    final SamplingConf samplingConf;
    final Sampler<Point> sampler;
    private final String metricName;
    protected final List<JsonObject> chunks = new ArrayList<>();
    final List<Point> sampledPoints = new ArrayList<>();
    private long totalChunkCounter = 0L;
    long toatlPointCounter = 0L;
    long pointCounter = 0L;

    public TimeSeriesExtracterImpl(String metricName, long from, long to,
                                   SamplingConf samplingConf,
                                   long totalNumberOfPoint) {
        this.metricName = metricName;
        this.from = from;
        this.to = to;
        this.samplingConf = TimeSeriesExtracterUtil.calculSamplingConf(samplingConf, totalNumberOfPoint);
        sampler = SamplerFactory.getPointSampler(this.samplingConf.getAlgo(), this.samplingConf.getBucketSize());
    }

    @Override
    public void addChunk(JsonObject chunk) {
        totalChunkCounter++;
        pointCounter+=chunk.getLong(HistorianFields.RESPONSE_CHUNK_SIZE_FIELD);
        chunks.add(chunk);
        if (isBufferFull()) {
            samplePointsInBufferThenReset();
        }
    }

    public boolean isBufferFull() {
        return pointCounter >= samplingConf.getBucketSize();
    }

    @Override
    public void flush() {
        if (!chunks.isEmpty())
            samplePointsInBufferThenReset();
    }

    protected void samplePointsInBufferThenReset() {
        LOGGER.trace("sample points in buffer has been called with chunks : {}",
                chunks.stream().map(JsonObject::encodePrettily).collect(Collectors.joining("\n")));
        Stream<Point> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints//TODO is sorting really necessary ?
                .sorted(Comparator.comparing(Point::getTimestamp));
        List<Point> sampledPoints = sampler.sample(sortedPoints.collect(Collectors.toList()));
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
