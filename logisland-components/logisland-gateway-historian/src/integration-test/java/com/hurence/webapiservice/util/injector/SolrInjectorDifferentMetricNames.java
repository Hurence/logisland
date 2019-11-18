package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModele;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SolrInjectorDifferentMetricNames extends AbstractSolrInjector {

    private final int size;
    private final int numberOfChunkByMetric;

    public SolrInjectorDifferentMetricNames(int numberOfMetric, int numberOfChunkByMetric) {
        this.size = numberOfMetric;
        this.numberOfChunkByMetric = numberOfChunkByMetric;
    }

    @Override
    protected List<ChunkModele> buildListOfChunks() {
        List<ChunkModele> chunks = IntStream.range(0, this.size)
                .mapToObj(i -> "metric_" + i)
                .map(this::buildChunkWithMetricName)
                .flatMap(this::createMoreChunkForMetric)
                .collect(Collectors.toList());
        return chunks;
    }

    private ChunkModele buildChunkWithMetricName(String metricName) {
        return ChunkModele.fromPoints(metricName, Arrays.asList(
                new Point(0, 1L, 5),
                new Point(0, 2L, 8),
                new Point(0, 3L, 1.2),
                new Point(0, 4L, 6.5)
        ));
    }

    private Stream<ChunkModele> createMoreChunkForMetric(ChunkModele chunk) {
        List<ChunkModele> chunks = IntStream.range(0, this.numberOfChunkByMetric)
                .mapToObj(i -> {
                    //TODO eventually change chunk content if needed
                    ChunkModele cloned = chunk;
                    return cloned;
                })
                .collect(Collectors.toList());
        return chunks.stream();
    }
}
