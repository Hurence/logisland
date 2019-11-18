package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModele;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPoints extends AbstractSolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;

    public SolrInjectorOneMetricMultipleChunksSpecificPoints(String metricName,
                                                             List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
    }

    @Override
    protected List<ChunkModele> buildListOfChunks() {
        List<ChunkModele> chunks = IntStream
                .range(0, pointsByChunk.size())
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModele buildChunk(int index) {
        return ChunkModele.fromPoints(metricName, pointsByChunk.get(index));
    }
}

