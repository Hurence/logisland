package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.util.List;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public class TimeSeriesExtracterUsingPreAgg extends TimeSeriesExtracterImpl {


    public TimeSeriesExtracterUsingPreAgg(String metricName, long from, long to, SamplingConf samplingConf, long totalNumberOfPoint) {
        super(metricName, from, to, samplingConf, totalNumberOfPoint);
    }

    @Override
    protected void samplePointsInBufferThenReset() {
        Point sampledPoint = sampleChunksIntoOneAggPoint(chunks);
        this.sampledPoints.add(sampledPoint);
        chunks.clear();
        toatlPointCounter+=pointCounter;
        pointCounter = 0;
    }

    private Point sampleChunksIntoOneAggPoint(List<JsonObject> chunks) {
        if (chunks.isEmpty())
            throw new IllegalArgumentException("chunks can not be empty !");
        long timestamp = chunks.stream()
                .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_START_FIELD))
                .findFirst()
                .getAsLong();
        double aggValue;
        switch (samplingConf.getAlgo()) {
            case AVERAGE:
                double sum = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_SUM_FIELD))
                        .sum();
                long numberOfPoint = chunks.stream()
                        .mapToLong(chunk -> chunk.getLong(RESPONSE_CHUNK_SIZE_FIELD))
                        .sum();
                aggValue = BigDecimal.valueOf(sum)
                        .divide(BigDecimal.valueOf(numberOfPoint))
                        .doubleValue();
                break;
            case FIRST_ITEM:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                        .findFirst()
                        .getAsDouble();
                break;
            case MIN:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MIN_FIELD))
                        .min()
                        .getAsDouble();
                break;
            case MAX:
                aggValue = chunks.stream()
                        .mapToDouble(chunk -> chunk.getDouble(RESPONSE_CHUNK_MAX_FIELD))
                        .max()
                        .getAsDouble();
                break;
            case MODE_MEDIAN:
            case LTTB:
            case MIN_MAX:
            case NONE:
            default:
                throw new IllegalStateException("Unsupported algo: " + samplingConf.getAlgo());
        }
        return new Point(0, timestamp, aggValue);
    }
}
