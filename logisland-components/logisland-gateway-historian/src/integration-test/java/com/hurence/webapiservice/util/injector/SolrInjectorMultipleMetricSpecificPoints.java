package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.util.modele.ChunkExpected;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorMultipleMetricSpecificPoints extends AbstractSolrInjector {

    private final static long MAX_POINTS_BY_METRIC = 100L;//It s a test utility....
    private final List<String> metricNames;
    private final List<List<Point>> pointsByMetric;

    public SolrInjectorMultipleMetricSpecificPoints(List<String> metricNames,
                                                    List<List<Point>> pointsByMetric) {
        this.metricNames = metricNames;
        this.pointsByMetric = pointsByMetric;
    }

    @Override
    protected List<ChunkExpected> buildListOfChunks() {
        List<ChunkExpected> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkExpected buildChunk(int index) {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = pointsByMetric.get(index);
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = chunk.points.stream().mapToLong(Point::getTimestamp).min().getAsLong();
        chunk.end = chunk.points.stream().mapToLong(Point::getTimestamp).max().getAsLong();;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.name = metricNames.get(index);
        chunk.sax = "edeebcccdf";
        return chunk;
    }
}
