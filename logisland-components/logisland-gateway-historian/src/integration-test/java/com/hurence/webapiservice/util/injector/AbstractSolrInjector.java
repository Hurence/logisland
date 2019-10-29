package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import com.hurence.webapiservice.util.injector.SolrInjector;
import com.hurence.webapiservice.util.modele.ChunkExpected;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

import static com.hurence.logisland.record.FieldDictionary.*;
import static com.hurence.logisland.record.FieldDictionary.CHUNK_SUM;
import static com.hurence.webapiservice.historian.HistorianService.METRIC_NAME;

public abstract class AbstractSolrInjector implements SolrInjector {

    protected int ddcThreshold = 0;
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION;

    protected byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<ChunkExpected> chunks = buildListOfChunks();
        for(int i = 0; i < chunks.size(); i++) {
            ChunkExpected chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION);
    }

    protected abstract List<ChunkExpected> buildListOfChunks();

    private SolrInputDocument buildSolrDocument(ChunkExpected chunk, String id) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField(CHUNK_START, chunk.start);
        doc.addField(CHUNK_SIZE, chunk.points.size());
        doc.addField(CHUNK_END, chunk.end);
        doc.addField(CHUNK_SAX, chunk.sax);
        doc.addField(CHUNK_VALUE, chunk.compressedPoints);
        doc.addField(CHUNK_AVG, chunk.avg);
        doc.addField(CHUNK_MIN, chunk.min);
        doc.addField(CHUNK_WINDOW_MS, 11855);
        doc.addField(METRIC_NAME, chunk.name);
        doc.addField(CHUNK_TREND, chunk.trend);
        doc.addField(CHUNK_MAX, chunk.max);
        doc.addField(CHUNK_SIZE_BYTES, chunk.compressedPoints.length);
        doc.addField(CHUNK_SUM, chunk.sum);
        return doc;
    }
}
