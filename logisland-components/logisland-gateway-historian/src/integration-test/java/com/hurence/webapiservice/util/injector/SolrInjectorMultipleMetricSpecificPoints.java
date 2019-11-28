package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModele;

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
    protected List<ChunkModele> buildListOfChunks() {
        List<ChunkModele> chunks = IntStream
                .range(0, Math.min(metricNames.size(), pointsByMetric.size()))
                .mapToObj(this::buildChunk)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModele buildChunk(int index) {
        return ChunkModele.fromPoints(metricNames.get(index), pointsByMetric.get(index));
    }
}
