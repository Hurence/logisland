package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.util.modele.ChunkModele;

import java.util.Arrays;
import java.util.List;

public class SolrInjectorTempaAndTempbSize4 extends SolrInjectorTempASize3 {

    @Override
    protected List<ChunkModele> buildListOfChunks() {
        List<ChunkModele> chunks = super.buildListOfChunks();
        chunks.add(buildChunk());
        return chunks;
    }

    private ChunkModele buildChunk() {
        ChunkModele chunk = new ChunkModele();
        chunk.points = Arrays.asList(
                new Point(0, 9L, -5),
                new Point(0, 10L, 80),
                new Point(0, 11L, 1.2),
                new Point(0, 12L, 5.5)
        );
        chunk.compressedPoints = compressPoints(chunk.points);
        chunk.start = 16L;
        chunk.end = 20L;
        chunk.sum = chunk.points.stream().mapToDouble(Point::getValue).sum();
        chunk.avg = chunk.sum / chunk.points.size();
        chunk.min = chunk.points.stream().mapToDouble(Point::getValue).min().getAsDouble();
        chunk.max = chunk.points.stream().mapToDouble(Point::getValue).max().getAsDouble();
        chunk.name = "temp_b";
        chunk.sax = "edeebcccdf";
        return chunk;
    }
}
