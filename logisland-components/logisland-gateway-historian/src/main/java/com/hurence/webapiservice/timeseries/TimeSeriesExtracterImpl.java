package com.hurence.webapiservice.timeseries;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.sampling.Sampler;
import com.hurence.logisland.timeseries.sampling.SamplerFactory;
import com.hurence.webapiservice.modele.SamplingConf;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TimeSeriesExtracterImpl extends AbstractTimeSeriesExtracter implements TimeSeriesExtracter {

    private static Logger LOGGER = LoggerFactory.getLogger(TimeSeriesExtracterImpl.class);

    final Sampler<Point> sampler;

    public TimeSeriesExtracterImpl(String metricName, long from, long to,
                                   SamplingConf samplingConf,
                                   long totalNumberOfPoint) {
        super(metricName, from, to, samplingConf, totalNumberOfPoint);
        sampler = SamplerFactory.getPointSampler(this.samplingConf.getAlgo(), this.samplingConf.getBucketSize());
    }

    @Override
    protected void samplePointsFromChunks(long from, long to, List<JsonObject> chunks) {
        Stream<Point> extractedPoints = TimeSeriesExtracterUtil.extractPointsAsStream(from, to, chunks);
        Stream<Point> sortedPoints = extractedPoints
                .sorted(Comparator.comparing(Point::getTimestamp));
        List<Point> sampledPoints = sampler.sample(sortedPoints.collect(Collectors.toList()));
        this.sampledPoints.addAll(sampledPoints);
    }
}
