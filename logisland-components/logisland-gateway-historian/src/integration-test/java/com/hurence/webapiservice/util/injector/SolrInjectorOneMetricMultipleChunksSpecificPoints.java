package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.util.modele.ChunkExpected;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractSolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final List<List<String>> tags;

    public SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                             List<List<String>> tags,
                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
        this.tags = tags;
    }

    @Override
    protected List<ChunkExpected> buildListOfChunks() {
        List<ChunkExpected> chunks = IntStream
                .range(0, Math.min(tags.size(), pointsByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkExpected buildChunk(int index) {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = pointsByChunk.get(index);
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = chunk.points.stream().mapToLong(Point::getTimestamp).min().getAsLong();
        chunk.end = chunk.points.stream().mapToLong(Point::getTimestamp).max().getAsLong();;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.name = metricName;
        chunk.sax = "edeebcccdf";
        chunk.tags = tags.get(index);
        return chunk;
    }
}

