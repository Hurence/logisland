package com.hurence.webapiservice.util.injector;

import com.hurence.logisland.record.Point;
import com.hurence.logisland.timeseries.converter.common.Compression;
import com.hurence.logisland.timeseries.converter.serializer.protobuf.ProtoBufMetricTimeSeriesSerializer;
import com.hurence.util.modele.ChunkModele;
import com.hurence.webapiservice.util.HistorianSolrITHelper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hurence.webapiservice.historian.HistorianFields.*;

public abstract class AbstractSolrInjector implements SolrInjector {

    protected int ddcThreshold = 0;
    private static String COLLECTION = HistorianSolrITHelper.COLLECTION;
    private List<ChunkModele> extraCustomChunks = new ArrayList<>();

    protected byte[] compressPoints(List<Point> pointsChunk) {
        byte[] serializedPoints = ProtoBufMetricTimeSeriesSerializer.to(pointsChunk.iterator(), ddcThreshold);
        return Compression.compress(serializedPoints);
    }

    @Override
    public void injectChunks(SolrClient client) throws SolrServerException, IOException {
        final List<ChunkModele> chunks = buildListOfChunks();
        chunks.addAll(extraCustomChunks);
        for(int i = 0; i < chunks.size(); i++) {
            ChunkModele chunkExpected = chunks.get(i);
            client.add(COLLECTION, buildSolrDocument(chunkExpected, "id" + i));
        }
        UpdateResponse updateRsp = client.commit(COLLECTION);
    }

    public void addChunk(ChunkModele chunk) {
        extraCustomChunks.add(chunk);
    }

    public void addChunk(AbstractSolrInjector injector) {
        extraCustomChunks.addAll(injector.buildListOfChunks());
    }

    protected abstract List<ChunkModele> buildListOfChunks();

    private SolrInputDocument buildSolrDocument(ChunkModele chunk, String id) {
        return chunk.buildSolrDocument(id);
    }
}
