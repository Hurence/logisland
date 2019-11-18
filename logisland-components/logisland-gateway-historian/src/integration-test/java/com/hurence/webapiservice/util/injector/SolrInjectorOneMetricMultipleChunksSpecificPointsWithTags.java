package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModele;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags extends AbstractSolrInjector {

    private final String metricName;
    private final List<List<Point>> pointsByChunk;
    private final List<List<String>> tags;

    public SolrInjectorOneMetricMultipleChunksSpecificPointsWithTags(String metricName,
                                                                     List<List<String>> tags,
                                                                     List<List<Point>> pointsByChunk) {
        this.metricName = metricName;
        this.pointsByChunk = pointsByChunk;
        this.tags = tags;
    }

    @Override
    protected List<ChunkModele> buildListOfChunks() {
        List<ChunkModele> chunks = IntStream
                .range(0, Math.min(tags.size(), pointsByChunk.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModele buildChunk(int index) {
        ChunkModele chunk = ChunkModele.fromPoints(metricName, pointsByChunk.get(index));
        chunk.tags = tags.get(index);
        return chunk;
    }
}

