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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractTimeSeriesExtracter.class);

    public static String TIMESERIE_NAME = "target";
    public static String TIMESERIE_POINT = "datapoints";
    public static String TIMESERIE_AGGS = "aggs";
    final long from;
    final long to;
    final SamplingConf samplingConf;
    private final String metricName;
    protected final List<JsonObject> chunks = new ArrayList<>();
    final List<Point> sampledPoints = new ArrayList<>();
    private long totalChunkCounter = 0L;
    long toatlPointCounter = 0L;
    long pointCounter = 0L;

    public AbstractTimeSeriesExtracter(String metricName, long from, long to,
                                       SamplingConf samplingConf,
                                       long totalNumberOfPoint) {
        this.metricName = metricName;
        this.from = from;
        this.to = to;
        this.samplingConf = TimeSeriesExtracterUtil.calculSamplingConf(samplingConf, totalNumberOfPoint);
        LOGGER.debug("Initialized {}  with samplingConf : {}", this.getClass(), this.samplingConf);
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
        return pointCounter > 4000 && pointCounter >= samplingConf.getBucketSize();
    }

    @Override
    public void flush() {
        if (!chunks.isEmpty())
            samplePointsInBufferThenReset();
    }

    /**
     * Extract/Sample points from the list of chunks in buffer using a strategy. Add them into sampled points then reset buffer
     */
    protected void samplePointsInBufferThenReset() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("sample points in buffer has been called with chunks : {}",
                    chunks.stream().map(JsonObject::encodePrettily).collect(Collectors.joining("\n")));
        }
        samplePointsFromChunks(from, to, chunks);
        chunks.clear();
        toatlPointCounter+=pointCounter;
        pointCounter = 0;
    }

    /**
     * Sample points from the list of chunks using a strategy. Add them into sampled points then reset buffer
     */
    protected abstract void samplePointsFromChunks(long from, long to, List<JsonObject> chunks);

    @Override
    public JsonObject getTimeSeries() {
        List<JsonArray> points = sampledPoints.stream()
                /*
                * Here we have to sort sampled points in case some chunks are intersecting.
                * The best would be to repare the chunk though. A mechanism that would track those chunks and rebuild them
                * may be the best solution I think. The requesting code here should suppose chunks are not intersecting.
                * We sort just so that user can not realize there is a problem in chunks.
                */
                .sorted(Comparator.comparing(Point::getTimestamp))
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
