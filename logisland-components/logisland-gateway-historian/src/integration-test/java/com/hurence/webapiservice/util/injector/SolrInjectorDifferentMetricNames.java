package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.webapiservice.util.modele.ChunkExpected;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorDifferentMetricNames extends AbstractSolrInjector {

    private final int size;

    public SolrInjectorDifferentMetricNames(int size) {
        this.size = size;
    }

    @Override
    protected List<ChunkExpected> buildListOfChunks() {
        List<ChunkExpected> chunks = IntStream.range(0, this.size)
                .mapToObj(i -> "metric_" + i)
                .map(this::buildChunkWithMetricName)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkExpected buildChunkWithMetricName(String metricName) {
        ChunkExpected chunk = new ChunkExpected();
        chunk.points = Arrays.asList(
                new Point(0, 1L, 5),
                new Point(0, 2L, 8),
                new Point(0, 3L, 1.2),
                new Point(0, 4L, 6.5)
        );
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = 1L;
        chunk.end = 4L;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.name = metricName;
        chunk.sax = "edeebcccdf";
        return chunk;
    }
}
